from flask import Flask, request, jsonify
import requests
import logging
import difflib
import re
import time
import json
import random
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import wraps

# Optional imports for advanced features - comment out if not needed
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Optional metrics collection - comment out if not needed
try:
    from datadog import statsd
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False

app = Flask(__name__)

# Base URL for the Octagon API
OCTAGON_API_BASE_URL = "https://api.octagon-api.com"

# Set up logging to debug
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("simple_mma_webhook")

# TTL Cache implementation
class TTLCache:
    def __init__(self, ttl=3600):
        self.cache = {}
        self.timestamps = {}
        self.ttl = ttl  # Time to live in seconds

    def get(self, key):
        if key in self.cache:
            # Check if cache entry is expired
            if time.time() - self.timestamps[key] < self.ttl:
                return self.cache[key]
            else:
                # Remove expired entry
                del self.cache[key]
                del self.timestamps[key]
        return None

    def set(self, key, value):
        self.cache[key] = value
        self.timestamps[key] = time.time()
        
    def clear(self):
        self.cache.clear()
        self.timestamps.clear()

# Rate limiter for API calls
class RateLimiter:
    def __init__(self, calls_per_second=5):
        self.calls_per_second = calls_per_second
        self.last_call_time = 0
        
    def wait_if_needed(self):
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        time_to_wait = (1.0 / self.calls_per_second) - time_since_last_call
        
        if time_to_wait > 0:
            time.sleep(time_to_wait)
            
        self.last_call_time = time.time()

# Create caches
CACHE = {
    "rankings": TTLCache(ttl=3600),  # 1 hour
    "fighters": TTLCache(ttl=86400),  # 24 hours
    "fighter_details": TTLCache(ttl=3600),  # 1 hour
    "division_details": TTLCache(ttl=3600),  # 1 hour
    "all_fighters_data": None,  # Cache for enriched fighter data
    "division_mapping": None    # Cache for division name mapping
}

# Optional Redis integration
if REDIS_AVAILABLE:
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        # Test connection
        redis_client.ping()
        REDIS_ENABLED = True
        logger.info("Redis connected successfully")
    except:
        REDIS_ENABLED = False
        logger.warning("Redis connection failed, falling back to in-memory cache")
else:
    REDIS_ENABLED = False
    redis_client = None

# Create a rate limiter
api_rate_limiter = RateLimiter(calls_per_second=3)

# Create a session with retries and connection pooling
def create_requests_session():
    session = requests.Session()
    
    # Configure retries - handle both older and newer versions of urllib3
    try:
        # Try newer versions first (urllib3 >= 2.0.0)
        retries = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
    except TypeError:
        # Fall back to older versions (urllib3 < 2.0.0)
        retries = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["GET"]
        )
    
    # Add the adapter with connection pooling
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

# Use this session for all requests
http_session = create_requests_session()

# Reference dictionaries for pattern matching
CHAMPION_KEYWORDS = ["champion", "champ", "title", "belt", "titleholder"]
FIGHTER_KEYWORDS = ["fighter", "athlete", "competitor", "contender"]
RANKING_KEYWORDS = ["ranking", "rankings", "top", "best fighter", "best fighters"]
RECORD_KEYWORDS = ["record", "stats", "statistics", "win", "wins", "loss", "losses"]
INFO_KEYWORDS = ["who is", "tell me about", "info", "information", "details"]
PHYSICAL_ATTRIBUTES = ["height", "weight", "reach", "leg reach", "tallest", "shortest", "longest", "heaviest", "lightest"]
FIGHTING_STYLES = ["wrestler", "boxing", "bjj", "jiu jitsu", "muay thai", "karate", "kickbox", "sambo", "wrestler", "kickboxing", "boxing", "judo"]

# Database of retired/famous fighters not in the API
RETIRED_FIGHTERS = {
    "anderson silva": {
        "name": "Anderson Silva",
        "nickname": "The Spider",
        "category": "Middleweight Division (Former)",
        "wins": "34",
        "losses": "11",
        "draws": "0",
        "status": "Retired",
        "fightingStyle": "Muay Thai, Brazilian Jiu-Jitsu",
        "age": "48",
        "height": "74.00",
        "weight": "185.00",
        "reach": "77.50",
        "legReach": "42.00",
        "placeOfBirth": "São Paulo, Brazil",
        "trainsAt": "Spider Kick (Formerly Team Nogueira)",
        "octagonDebut": "Jun. 28, 2006",
        "notes": "Former UFC Middleweight Champion with the longest title reign in UFC history (2,457 days)"
    },
    "georges st pierre": {
        "name": "Georges St-Pierre",
        "nickname": "GSP",
        "category": "Welterweight Division (Former)",
        "wins": "26",
        "losses": "2",
        "draws": "0",
        "status": "Retired",
        "fightingStyle": "Wrestling, Boxing, Kyokushin Karate",
        "age": "42",
        "height": "70.00",
        "weight": "170.00",
        "reach": "76.00",
        "legReach": "41.00",
        "placeOfBirth": "Saint-Isidore, Quebec, Canada",
        "trainsAt": "Tristar Gym",
        "octagonDebut": "Jan. 31, 2004",
        "notes": "Former UFC Welterweight and Middleweight Champion, widely considered one of the greatest MMA fighters of all time"
    },
    "khabib nurmagomedov": {
        "name": "Khabib Nurmagomedov",
        "nickname": "The Eagle",
        "category": "Lightweight Division (Former)",
        "wins": "29",
        "losses": "0",
        "draws": "0",
        "status": "Retired",
        "fightingStyle": "Sambo, Wrestling, Judo",
        "age": "35",
        "height": "70.00",
        "weight": "155.00",
        "reach": "70.00",
        "legReach": "40.00",
        "placeOfBirth": "Dagestan, Russia",
        "trainsAt": "American Kickboxing Academy",
        "octagonDebut": "Jan. 20, 2012",
        "notes": "Retired as undefeated UFC Lightweight Champion, known for his dominant grappling"
    },
    "demetrious johnson": {
        "name": "Demetrious Johnson",
        "nickname": "Mighty Mouse",
        "category": "Flyweight Division (Former)",
        "wins": "30",
        "losses": "4",
        "draws": "1",
        "status": "Active in ONE Championship",
        "fightingStyle": "Wrestling, Muay Thai, Brazilian Jiu-Jitsu",
        "age": "37",
        "height": "64.00",
        "weight": "125.00",
        "reach": "66.00",
        "legReach": "32.00",
        "placeOfBirth": "Madisonville, Kentucky, USA",
        "trainsAt": "AMC Pankration",
        "octagonDebut": "Feb. 5, 2011",
        "notes": "Former UFC Flyweight Champion with the most consecutive title defenses (11) in UFC history"
    }
}

# Famous fighters by nickname, for better matching
FIGHTER_NICKNAMES = {
    "the spider": "anderson silva",
    "gsp": "georges st pierre",
    "the eagle": "khabib nurmagomedov",
    "mighty mouse": "demetrious johnson",
    "bones": "jon jones",
    "notorious": "conor mcgregor",
    "stylebender": "israel adesanya",
    "the last stylebender": "israel adesanya",
    "poatan": "alex pereira",
    "reaper": "robert whittaker",
    "lionheart": "anthony smith",
    "borz": "khamzat chimaev",
    "el cucuy": "tony ferguson",
    "thug": "rose namajunas",
    "thug rose": "rose namajunas",
    "blessed": "max holloway",
    "the diamond": "dustin poirier"
}

# Standalone last names mapping
STANDALONE_LAST_NAMES = {
    "cejudo": "henry-cejudo",
    "jones": "jon-jones",
    "adesanya": "israel-adesanya",
    "mcgregor": "conor-mcgregor",
    "poirier": "dustin-poirier",
    "holloway": "max-holloway",
    "oliveira": "charles-oliveira",
    "elliott": "tim-elliott",
    "pantoja": "alexandre-pantoja",
    "moreno": "brandon-moreno",
    "makhachev": "islam-makhachev",
    "volkanovski": "alexander-volkanovski",
    "pereira": "alex-pereira",
    "whittaker": "robert-whittaker",
    "chimaev": "khamzat-chimaev",
    "ngannou": "francis-ngannou",
    "cormier": "daniel-cormier",
    "stipe": "stipe-miocic",
    "miocic": "stipe-miocic",
    "usman": "kamaru-usman",
    "masvidal": "jorge-masvidal",
    "namajunas": "rose-namajunas",
    "shevchenko": "valentina-shevchenko"
}

# Weight class mapping for identification
WEIGHT_CLASS_MAPPING = {
    # Exact divisions
    "flyweight": "flyweight",
    "bantamweight": "bantamweight",
    "featherweight": "featherweight",
    "lightweight": "lightweight",
    "welterweight": "welterweight",
    "middleweight": "middleweight",
    "light heavyweight": "light-heavyweight",
    "light-heavyweight": "light-heavyweight",
    "heavyweight": "heavyweight",
    
    # Approximate weights
    "125": "flyweight",
    "135": "bantamweight",
    "145": "featherweight",
    "155": "lightweight",
    "170": "welterweight",
    "185": "middleweight",
    "205": "light-heavyweight",
    "265": "heavyweight",
    
    # Women's divisions
    "women's strawweight": "womens-strawweight",
    "womens strawweight": "womens-strawweight",
    "women's flyweight": "womens-flyweight",
    "womens flyweight": "womens-flyweight",
    "women's bantamweight": "womens-bantamweight",
    "womens bantamweight": "womens-bantamweight",
    
    # Common abbreviations
    "fly": "flyweight",
    "bantam": "bantamweight",
    "feather": "featherweight",
    "light": "lightweight",
    "welter": "welterweight",
    "middle": "middleweight",
    "lhw": "light-heavyweight",
    "hw": "heavyweight",
    
    # Pound for pound
    "pound-for-pound": "mens-pound-for-pound-top-rank",
    "pound for pound": "mens-pound-for-pound-top-rank",
    "p4p": "mens-pound-for-pound-top-rank",
    "men's p4p": "mens-pound-for-pound-top-rank",
    "mens p4p": "mens-pound-for-pound-top-rank",
    "women's p4p": "womens-pound-for-pound-top-rank",
    "womens p4p": "womens-pound-for-pound-top-rank"
}

# Compiled Regex patterns for better performance
CHAMPION_PATTERNS = [
    re.compile(r"(?:who|what|which|whos|who's) (?:is|are) (?:the)?\s*([a-zA-Z'\s]+)\s*(?:champion|champ|title|belt)(?:ion)?(?:holder)?"),
    re.compile(r"([a-zA-Z'\s]+)\s*(?:champion|champ|title|belt)(?:ion)?(?:holder)?")
]

RANKING_PATTERNS = [
    re.compile(r"([a-zA-Z'\s]+)\s*(?:ranking|rankings)"),
    re.compile(r"(?:ranking|rankings)(?:\s+for)?\s+([a-zA-Z'\s]+)")
]

RECORD_PATTERNS = [
    re.compile(r"(?:record|stats) of ([a-zA-Z'\s]+)"),
    re.compile(r"([a-zA-Z'\s]+)(?:'s)? (?:record|stats)"),
    re.compile(r"(?:what is|how good is) ([a-zA-Z'\s]+)(?:'s)? record")
]

FIGHTER_PATTERNS = [
    re.compile(r"(?:who|what|which|tell me about|info on|information about)\s+(?:is|are|on|about)?\s*([a-zA-Z'\s]+)(?:\?|$)"),
    re.compile(r"([a-zA-Z'\s]+)(?:'s)?\s*(?:record|stats|statistics|profile|information|details)")
]

P4P_PATTERNS = [
    re.compile(r"(?:men(?:'s)?|male)?\s*pound\s*(?:for|4)\s*pound\s*(?:ranking|rankings)?"),
    re.compile(r"p4p\s*(?:ranking|rankings)?"),
    re.compile(r"pound\s*(?:for|4)\s*pound\s*(?:ranking|rankings)?")
]

COMPARISON_PATTERNS = [
    re.compile(r"(?:who is |who's |which is |which one is )?(taller|shorter|heavier|lighter|bigger|stronger|better)(?: |,)(?:between )?([\w\s']+) (?:or|and|vs\.?) ([\w\s']+)"),
    re.compile(r"(?:compare|vs|versus) ([\w\s']+) (?:and|vs\.?) ([\w\s']+)"),
    re.compile(r"([\w\s']+) (?:vs\.?|versus|compared to|against) ([\w\s']+)")
]

WEIGHT_CLASS_PATTERNS = [
    re.compile(r'(\d+)\s*(?:pound|lb|lbs)'),
    re.compile(r'(\d+)\s*kg')
]

# Timing decorator for metrics collection
def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        # Log timing for debugging
        logger.debug(f"{func.__name__} execution time: {execution_time:.4f} seconds")
        
        # Send metric if available
        if METRICS_AVAILABLE:
            statsd.timing(f'webhook.{func.__name__}.execution_time', execution_time)
        
        return result
    return wrapper

def sanitize_input(text):
    """Sanitize user input to prevent injection and control character issues"""
    if text is None:
        return ""
        
    # Remove any control characters
    text = ''.join(char for char in text if ord(char) >= 32)
    
    # Limit length
    MAX_INPUT_LENGTH = 200
    if len(text) > MAX_INPUT_LENGTH:
        text = text[:MAX_INPUT_LENGTH]
        
    return text

@timing_decorator
def load_fighters_data():
    """Load all fighters data from the API and cache it."""
    # Check Redis first if enabled
    if REDIS_ENABLED:
        cached_data = redis_client.get("fighters_data")
        if cached_data:
            try:
                CACHE["fighters"].set("fighters", json.loads(cached_data))
                logger.debug("Loaded fighters data from Redis")
                return
            except Exception as e:
                logger.error(f"Error loading fighters from Redis: {e}")
    
    # If not in Redis or Redis failed, check memory cache
    if CACHE["fighters"].get("fighters"):
        return
    
    # Fetch from API if not in cache
    fighters_url = f"{OCTAGON_API_BASE_URL}/fighters"
    try:
        api_rate_limiter.wait_if_needed()
        response = http_session.get(fighters_url, timeout=5)
        response.raise_for_status()
        fighters_data = response.json()
        
        # Store in memory cache
        CACHE["fighters"].set("fighters", fighters_data)
        
        # Store in Redis if enabled
        if REDIS_ENABLED:
            try:
                redis_client.setex("fighters_data", 86400, json.dumps(fighters_data))  # 24 hour TTL
            except Exception as e:
                logger.error(f"Error storing fighters in Redis: {e}")
                
        logger.debug(f"Loaded {len(fighters_data)} fighters from API")
    except requests.exceptions.Timeout:
        logger.error("Timeout fetching fighters data")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching fighters data: {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching fighters data: {e}")
    except ValueError as e:
        logger.error(f"JSON parsing error for fighters data: {e}")

@timing_decorator
def load_rankings_data():
    """Load all rankings data from the API and cache it."""
    # Check Redis first if enabled
    if REDIS_ENABLED:
        cached_data = redis_client.get("rankings_data")
        if cached_data:
            try:
                CACHE["rankings"].set("rankings", json.loads(cached_data))
                logger.debug("Loaded rankings data from Redis")
                return
            except Exception as e:
                logger.error(f"Error loading rankings from Redis: {e}")
    
    # If not in Redis or Redis failed, check memory cache
    if CACHE["rankings"].get("rankings"):
        return
    
    # Fetch from API if not in cache
    rankings_url = f"{OCTAGON_API_BASE_URL}/rankings"
    try:
        api_rate_limiter.wait_if_needed()
        response = http_session.get(rankings_url, timeout=5)
        response.raise_for_status()
        rankings_data = response.json()
        
        # Store in memory cache
        CACHE["rankings"].set("rankings", rankings_data)
        
        # Store in Redis if enabled
        if REDIS_ENABLED:
            try:
                redis_client.setex("rankings_data", 3600, json.dumps(rankings_data))  # 1 hour TTL
            except Exception as e:
                logger.error(f"Error storing rankings in Redis: {e}")
                
        logger.debug(f"Loaded {len(rankings_data)} rankings from API")
    except requests.exceptions.Timeout:
        logger.error("Timeout fetching rankings data")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching rankings data: {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching rankings data: {e}")
    except ValueError as e:
        logger.error(f"JSON parsing error for rankings data: {e}")

@timing_decorator
def get_all_fighters_data():
    """Combine and cache all fighter data for comparisons."""
    # Check if we already have the combined data cached
    if CACHE["all_fighters_data"]:
        return CACHE["all_fighters_data"]
    
    # Check Redis first if enabled
    if REDIS_ENABLED:
        cached_data = redis_client.get("all_fighters_data")
        if cached_data:
            try:
                all_fighters = json.loads(cached_data)
                CACHE["all_fighters_data"] = all_fighters
                logger.debug("Loaded all fighters data from Redis")
                return all_fighters
            except Exception as e:
                logger.error(f"Error loading all fighters from Redis: {e}")
    
    # Load fighters data if not already loaded
    load_fighters_data()
    fighters_data = CACHE["fighters"].get("fighters")
    if not fighters_data:
        logger.error("Failed to get fighters data")
        return []
    
    # Combine API data with retired fighters
    all_fighters = []
    
    # Add fighters from API
    for fighter_id, fighter_data in fighters_data.items():
        # Get full details for the fighter
        details = get_fighter_data(fighter_id)
        if details:
            # Try to safely convert string values to float
            try:
                height = float(str(details.get('height', '0')).replace('"', ''))
            except (ValueError, TypeError):
                height = 0
                
            try:
                weight = float(details.get('weight', '0') or 0)
            except (ValueError, TypeError):
                weight = 0
                
            try:
                reach = float(str(details.get('reach', '0')).replace('"', ''))
            except (ValueError, TypeError):
                reach = 0
                
            try:
                leg_reach = float(str(details.get('legReach', '0')).replace('"', ''))
            except (ValueError, TypeError):
                leg_reach = 0
            
            fighter_info = {
                "id": fighter_id,
                "name": details.get("name", ""),
                "height": height,
                "weight": weight,
                "reach": reach,
                "legReach": leg_reach,
                "status": details.get("status", ""),
                "category": details.get("category", ""),
                "fightingStyle": details.get("fightingStyle", "")
            }
            all_fighters.append(fighter_info)
    
    # Add retired fighters
    for fighter_id, fighter_data in RETIRED_FIGHTERS.items():
        # Safe conversion for retired fighters too
        try:
            height = float(str(fighter_data.get("height", "0")).replace('"', ''))
        except (ValueError, TypeError):
            height = 0
            
        try:
            weight = float(fighter_data.get("weight", "0") or 0)
        except (ValueError, TypeError):
            weight = 0
            
        try:
            reach = float(str(fighter_data.get("reach", "0")).replace('"', ''))
        except (ValueError, TypeError):
            reach = 0
            
        try:
            leg_reach = float(str(fighter_data.get("legReach", "0")).replace('"', ''))
        except (ValueError, TypeError):
            leg_reach = 0
        
        fighter_info = {
            "id": fighter_id,
            "name": fighter_data.get("name", ""),
            "height": height,
            "weight": weight,
            "reach": reach,
            "legReach": leg_reach,
            "status": fighter_data.get("status", ""),
            "category": fighter_data.get("category", ""),
            "fightingStyle": fighter_data.get("fightingStyle", "")
        }
        all_fighters.append(fighter_info)
    
    # Store in memory cache
    CACHE["all_fighters_data"] = all_fighters
    
    # Store in Redis if enabled
    if REDIS_ENABLED:
        try:
            redis_client.setex("all_fighters_data", 3600, json.dumps(all_fighters))  # 1 hour TTL
        except Exception as e:
            logger.error(f"Error storing all fighters in Redis: {e}")
    
    return all_fighters

@timing_decorator
def build_division_mapping():
    """
    Build a comprehensive mapping of division names to IDs
    """
    if CACHE["division_mapping"]:
        return CACHE["division_mapping"]
        
    # Start with predefined mapping
    division_mapping = WEIGHT_CLASS_MAPPING.copy()
    
    # Add mappings from actual API data
    load_rankings_data()
    rankings_data = CACHE["rankings"].get("rankings")
    if rankings_data:
        for division in rankings_data:
            category_name = division.get("categoryName", "").lower()
            division_id = division.get("id", "")
            if category_name and division_id:
                division_mapping[category_name] = division_id
                
                # Also add variants without spaces
                division_mapping[category_name.replace(" ", "")] = division_id
                
                # Add weight class shorthand (e.g., "light heavy" for "light heavyweight")
                if "weight" in category_name:
                    base_name = category_name.replace("weight", "").strip()
                    division_mapping[base_name] = division_id
    
    # Cache the mapping
    CACHE["division_mapping"] = division_mapping
    return division_mapping

@timing_decorator
def normalize_division_name(division_name):
    """
    Convert any division name variation to the correct API division ID
    by checking against actual API data.
    """
    if not division_name:
        return None
    
    # Get our comprehensive division mapping
    division_mapping = build_division_mapping()
    
    # Special case for pound for pound
    if "pound" in division_name.lower() or "p4p" in division_name.lower():
        if "women" in division_name.lower() or "female" in division_name.lower():
            return "womens-pound-for-pound-top-rank"
        else:
            # Default to men's P4P if not specified
            return "mens-pound-for-pound-top-rank"
    
    # Normalize input
    search_name = division_name.lower().strip()
    
    # Special case for "light heavyweight" / "light heavy"
    if "light" in search_name and "heavy" in search_name:
        return "light-heavyweight"
    
    # Check direct matches in our mapping
    if search_name in division_mapping:
        return division_mapping[search_name]
    
    # Check for partial matches
    for name, div_id in division_mapping.items():
        if search_name in name or name in search_name:
            return div_id
    
    # Attempt fuzzy matching for inexact matches
    matches = difflib.get_close_matches(search_name, division_mapping.keys(), n=1, cutoff=0.7)
    if matches:
        return division_mapping[matches[0]]
    
    return None

@timing_decorator
def identify_weight_class(weight_mention):
    """Map approximate weight mentions to appropriate UFC weight classes."""
    if not weight_mention:
        return None
        
    weight_mention = weight_mention.lower().strip()
    
    # Check our comprehensive division mapping first
    division_mapping = build_division_mapping()
    if weight_mention in division_mapping:
        return division_mapping[weight_mention]
    
    # Search for numeric weight mentions
    for pattern in WEIGHT_CLASS_PATTERNS:
        match = pattern.search(weight_mention)
        if match:
            weight = match.group(1)
            # Convert to proper weight class
            if weight:
                weight_num = int(weight)
                if pattern.pattern.find('kg') > -1:
                    # Convert kg to lbs
                    weight_num = int(weight_num * 2.20462)
                
                # Map to weight class
                if weight_num <= 125:
                    return "flyweight"
                elif weight_num <= 135:
                    return "bantamweight"
                elif weight_num <= 145:
                    return "featherweight"
                elif weight_num <= 155:
                    return "lightweight"
                elif weight_num <= 170:
                    return "welterweight"
                elif weight_num <= 185:
                    return "middleweight"
                elif weight_num <= 205:
                    return "light-heavyweight"
                else:
                    return "heavyweight"
    
    # Try fuzzy matching as a last resort
    matches = difflib.get_close_matches(weight_mention, division_mapping.keys(), n=1, cutoff=0.6)
    if matches:
        return division_mapping[matches[0]]
    
    return None

@timing_decorator
def resolve_fighter_name(name):
    """
    Resolve a fighter name from user input to the correct fighter ID.
    Handles famous fighters, current fighters, and partial matches.
    """
    if not name:
        return None
        
    # Normalize the name
    name_lower = name.lower().strip()
    
    # Lazily initialize the name mapping on first call
    if not hasattr(resolve_fighter_name, "name_map"):
        resolve_fighter_name.name_map = {}
        
        # Add nicknames
        for nickname, fighter_name in FIGHTER_NICKNAMES.items():
            resolve_fighter_name.name_map[nickname] = fighter_name
            
        # Add standalone last names
        for last_name, fighter_id in STANDALONE_LAST_NAMES.items():
            resolve_fighter_name.name_map[last_name] = fighter_id
            
        # Add retired fighters
        for retired_id in RETIRED_FIGHTERS:
            resolve_fighter_name.name_map[retired_id] = f"retired:{retired_id}"
    
    # Check for nicknames first
    for nickname, fighter_name in FIGHTER_NICKNAMES.items():
        if nickname in name_lower:
            name_lower = fighter_name
            break
    
    # Direct map lookup first (fast)
    if name_lower in resolve_fighter_name.name_map:
        return resolve_fighter_name.name_map[name_lower]
    
    # Check retired fighters database
    for retired_id in RETIRED_FIGHTERS:
        if retired_id in name_lower or name_lower in retired_id:
            return f"retired:{retired_id}"
    
    # Make sure we have fighter data loaded
    load_fighters_data()
    fighters_data = CACHE["fighters"].get("fighters")
    
    # Add active fighters to the map if not done yet
    if fighters_data and len(resolve_fighter_name.name_map) < len(FIGHTER_NICKNAMES) + len(STANDALONE_LAST_NAMES) + len(RETIRED_FIGHTERS) + 10:
        for fighter_id, details in fighters_data.items():
            fighter_name = details.get("name", "").lower()
            if fighter_name:
                resolve_fighter_name.name_map[fighter_name] = fighter_id
    
    # Only proceed if we have fighters data
    if not fighters_data:
        return None
    
    # Check for fighting style mentions
    mentioned_style = None
    for style in FIGHTING_STYLES:
        if style in name_lower:
            mentioned_style = style
            # Remove style from name for better matching
            name_lower = name_lower.replace(style, "").strip()
            break
    
    # Check for exact match
    for fighter_id, details in fighters_data.items():
        if details.get("name", "").lower() == name_lower:
            resolve_fighter_name.name_map[name_lower] = fighter_id  # Cache for future
            return fighter_id
    
    # Check for names containing the input as a substring with more sophisticated scoring
    matches = []
    for fighter_id, details in fighters_data.items():
        fighter_name = details.get("name", "").lower()
        nickname = details.get("nickname", "").lower()
        style = details.get("fightingStyle", "").lower()
        
        # Calculate base name similarity
        name_score = difflib.SequenceMatcher(None, name_lower, fighter_name).ratio()
        
        # Check nickname match
        nickname_score = 0
        if nickname and (nickname in name_lower or name_lower in nickname):
            nickname_score = 0.8
        
        # Check style match
        style_score = 0
        if mentioned_style and mentioned_style in style:
            style_score = 0.3
        
        # Combined score with weighted components
        combined_score = max(name_score, nickname_score) + style_score * 0.5
        
        if combined_score > 0.6:  # Reasonable threshold
            matches.append((fighter_id, combined_score))
            
        # Also check individual name parts (first name, last name)
        name_parts = fighter_name.split()
        for part in name_parts:
            if name_lower == part or (len(name_lower) > 3 and name_lower in part):
                part_score = difflib.SequenceMatcher(None, name_lower, part).ratio()
                matches.append((fighter_id, part_score))
                break
    
    # Sort by similarity score (highest first)
    matches.sort(key=lambda x: x[1], reverse=True)
    
    # Return the best match if we have one
    if matches:
        best_match_id = matches[0][0]
        resolve_fighter_name.name_map[name_lower] = best_match_id  # Cache for future
        return best_match_id
    
    return None

@timing_decorator
def is_physical_attribute_query(message):
    """Check if a message is about physical attributes like height, reach, etc."""
    message_lower = message.lower()
    
    # Check for physical attribute keywords
    has_attribute = False
    for attr in PHYSICAL_ATTRIBUTES:
        if attr in message_lower:
            has_attribute = True
            break
    
    if not has_attribute:
        return False, None
    
    # Check for comparison terms
    comparison_terms = ["tallest", "shortest", "longest", "heaviest", "lightest", "biggest", "smallest"]
    
    for term in comparison_terms:
        if term in message_lower:
            attribute = None
            if term in ["tallest", "shortest"]:
                attribute = "height"
            elif term in ["longest"]:
                if "leg" in message_lower or "kick" in message_lower:
                    attribute = "legReach"
                else:
                    attribute = "reach"
            elif term in ["heaviest", "lightest"]:
                attribute = "weight"
            elif term in ["biggest", "smallest"]:
                if "leg" in message_lower or "kick" in message_lower:
                    attribute = "legReach"
                elif "arm" in message_lower:
                    attribute = "reach"
                else:
                    attribute = "height"  # Default to height
            
            if attribute:
                return True, {"attribute": attribute, "comparison": term}
    
    # Check for direct attribute mentions
    for attr in ["height", "weight", "reach"]:
        if attr in message_lower:
            return True, {"attribute": attr, "comparison": None}
    
    if "leg reach" in message_lower or "leg-reach" in message_lower:
        return True, {"attribute": "legReach", "comparison": None}
    
    return False, None

@timing_decorator
def parse_fighter_comparison(message):
    """Parse a comparison query between two fighters."""
    message_lower = message.lower().strip()
    
    for pattern in COMPARISON_PATTERNS:
        match = pattern.search(message_lower)
        if match:
            # For first pattern, we have comparison type and two names
            if len(match.groups()) >= 3:
                comparison = match.group(1)
                fighter1_name = match.group(2)
                fighter2_name = match.group(3)
            else:
                # For other patterns, just two names
                fighter1_name = match.group(1)
                fighter2_name = match.group(2)
                comparison = None
            
            fighter1_id = resolve_fighter_name(fighter1_name)
            fighter2_id = resolve_fighter_name(fighter2_name)
            
            if fighter1_id and fighter2_id:
                # Try to determine what attribute to compare
                attribute = None
                if comparison:
                    if comparison in ["taller", "shorter"]:
                        attribute = "height"
                    elif comparison in ["heavier", "lighter"]:
                        attribute = "weight"
                    elif comparison in ["bigger"]:
                        attribute = "height"  # Default to height
                    elif comparison in ["stronger", "better"]:
                        attribute = "record"  # Will trigger overall comparison
                        
                if not attribute:
                    if "taller" in message_lower or "shorter" in message_lower or "height" in message_lower:
                        attribute = "height"
                    elif "heavier" in message_lower or "lighter" in message_lower or "weight" in message_lower:
                        attribute = "weight"
                    elif "reach" in message_lower or "longer arms" in message_lower:
                        attribute = "reach"
                    elif "leg reach" in message_lower or "longer legs" in message_lower:
                        attribute = "legReach"
                
                return {
                    "intent": "fighter_comparison",
                    "fighter1_id": fighter1_id,
                    "fighter2_id": fighter2_id,
                    "attribute": attribute
                }
    
    return None

@timing_decorator
def parse_open_query(message):
    """Parse vague or open-ended queries to determine user intent."""
    message_lower = message.lower().strip()
    
    # Look for general topic indicators
    if re.search(r'\b(best|top|greatest|goat)\b', message_lower):
        # This is likely about rankings or pound-for-pound
        if re.search(r'\b(woman|women|female)\b', message_lower):
            return {"intent": "division_rankings", "division_id": "womens-pound-for-pound-top-rank"}
        else:
            return {"intent": "division_rankings", "division_id": "mens-pound-for-pound-top-rank"}
    
    # Check for weight class mentions by number
    weight_match = re.search(r'(\d+)\s*(?:pound|lb|lbs|kg|kilo)', message_lower)
    if weight_match:
        division_id = identify_weight_class(message_lower)
        if division_id:
            # If champion is mentioned
            if re.search(r'\b(champ|champion|title|belt)\b', message_lower):
                return {"intent": "division_champion", "division_id": division_id}
            else:
                return {"intent": "division_rankings", "division_id": division_id}
    
    # Check for explicit weight class mentions
    division_id = identify_weight_class(message_lower)
    if division_id:
        # If champion is mentioned
        if re.search(r'\b(champ|champion|title|belt)\b', message_lower):
            return {"intent": "division_champion", "division_id": division_id}
        else:
            return {"intent": "division_rankings", "division_id": division_id}
    
    # Check for physical attribute extremes
    attribute_extremes = {
        r'\btallest\b': {"attribute": "height", "comparison": "tallest"},
        r'\bshortest\b': {"attribute": "height", "comparison": "shortest"},
        r'\bheaviest\b': {"attribute": "weight", "comparison": "heaviest"},
        r'\blightest\b': {"attribute": "weight", "comparison": "lightest"},
        r'\blongest reach\b': {"attribute": "reach", "comparison": "longest"},
        r'\blongest arms\b': {"attribute": "reach", "comparison": "longest"},
        r'\blongest legs\b': {"attribute": "legReach", "comparison": "longest"}
    }
    
    for pattern, attribute_data in attribute_extremes.items():
        if re.search(pattern, message_lower):
            return {"intent": "physical_comparison", "attribute_data": attribute_data}
    
    # If message seems to be about MMA but we can't determine specific intent
    mma_keywords = ["fight", "fighter", "ufc", "mma", "octagon", "knockout", "submission"]
    if any(word in message_lower for word in mma_keywords):
        return {"intent": "general_mma_question"}
    
    return {"intent": "unknown"}

@timing_decorator
def parse_query_intent(message):
    """
    Parse the user's message to determine intent and extract relevant entities.
    Returns a dictionary with the intent and any extracted entities.
    """
    if not message:
        return {"intent": "unknown"}
        
    message_lower = message.lower().strip()
    
    # Try to parse fighter comparison queries (new)
    comparison_result = parse_fighter_comparison(message_lower)
    if comparison_result:
        return comparison_result
    
    # Check for fighter record queries
    for pattern in RECORD_PATTERNS:
        match = pattern.search(message_lower)
        if match:
            fighter_name = match.group(1).strip()
            fighter_id = resolve_fighter_name(fighter_name)
            
            if fighter_id:
                return {
                    "intent": "fighter_info",
                    "fighter_id": fighter_id
                }
    
    # Handle single word fighter names (like "Cejudo?")
    if len(message_lower.split()) == 1 and message_lower.endswith('?'):
        fighter_name = message_lower.rstrip('?').strip()
        if len(fighter_name) > 3:  # Avoid short words
            fighter_id = resolve_fighter_name(fighter_name)
            if fighter_id:
                return {
                    "intent": "fighter_info",
                    "fighter_id": fighter_id
                }
    
    # Check for physical attribute comparisons
    is_physical, attribute_data = is_physical_attribute_query(message_lower)
    if is_physical and attribute_data and attribute_data.get("comparison"):
        return {
            "intent": "physical_comparison",
            "attribute_data": attribute_data
        }
    
    # Check for pound-for-pound rankings specific queries
    for pattern in P4P_PATTERNS:
        if pattern.search(message_lower):
            # Check if men's or women's is specified
            if "women" in message_lower or "female" in message_lower:
                return {
                    "intent": "division_rankings",
                    "division_id": "womens-pound-for-pound-top-rank"
                }
            else:
                # Default to men's P4P
                return {
                    "intent": "division_rankings",
                    "division_id": "mens-pound-for-pound-top-rank"
                }
    
    # Division champion pattern: "who is [division] champion" or "[division] champion"
    for pattern in CHAMPION_PATTERNS:
        match = pattern.search(message_lower)
        if match:
            division_name = match.group(1).strip()
            division_id = normalize_division_name(division_name)
            
            # If we found a valid division, return division champion intent
            if division_id:
                return {
                    "intent": "division_champion",
                    "division_id": division_id
                }
    
    # Rankings pattern: "[division] rankings" or "rankings for [division]"
    for pattern in RANKING_PATTERNS:
        match = pattern.search(message_lower)
        if match:
            division_name = match.group(1).strip()
            division_id = normalize_division_name(division_name)
            
            # If we found a valid division, return division rankings intent
            if division_id:
                return {
                    "intent": "division_rankings",
                    "division_id": division_id
                }
    
    # Check for physical attributes of specific fighters
    if is_physical and attribute_data:
        # Look for a fighter name in the message
        fighter_patterns = [
            re.compile(r"(?:height|weight|reach|leg reach) of ([a-zA-Z'\s]+)"),
            re.compile(r"([a-zA-Z'\s]+)(?:'s)?\s*(?:height|weight|reach|leg reach)")
        ]
        
        for pattern in fighter_patterns:
            match = pattern.search(message_lower)
            if match:
                fighter_name = match.group(1).strip()
                fighter_id = resolve_fighter_name(fighter_name)
                
                if fighter_id:
                    return {
                        "intent": "fighter_attribute",
                        "fighter_id": fighter_id,
                        "attribute": attribute_data.get("attribute")
                    }
    
    # Fighter info pattern: "who is [fighter]" or "tell me about [fighter]"
    for pattern in FIGHTER_PATTERNS:
        match = pattern.search(message_lower)
        if match:
            fighter_name = match.group(1).strip()
            fighter_id = resolve_fighter_name(fighter_name)
            
            # If we found a valid fighter, return fighter info intent
            if fighter_id:
                return {
                    "intent": "fighter_info",
                    "fighter_id": fighter_id
                }
    
    # Check if message is just about rankings
    if any(keyword in message_lower for keyword in RANKING_KEYWORDS):
        # Load rankings if not already loaded
        load_rankings_data()
        rankings_data = CACHE["rankings"].get("rankings")
        
        # See if there's a division mentioned
        if rankings_data:
            for division in rankings_data:
                division_name = division.get("categoryName", "").lower()
                if division_name in message_lower:
                    return {
                        "intent": "division_rankings",
                        "division_id": division.get("id")
                    }
        
        # No specific division mentioned
        return {"intent": "all_rankings"}
    
    # Check if message is just about champions
    if any(keyword in message_lower for keyword in CHAMPION_KEYWORDS):
        # Load rankings if not already loaded
        load_rankings_data()
        rankings_data = CACHE["rankings"].get("rankings")
        
        # See if there's a division mentioned
        if rankings_data:
            for division in rankings_data:
                division_name = division.get("categoryName", "").lower()
                if division_name in message_lower:
                    return {
                        "intent": "division_champion",
                        "division_id": division.get("id")
                    }
        
        # No specific division mentioned
        return {"intent": "all_champions"}
    
    # Try weight class identification for division-related queries
    division_id = identify_weight_class(message_lower)
    if division_id:
        if any(word in message_lower for word in CHAMPION_KEYWORDS):
            return {"intent": "division_champion", "division_id": division_id}
        else:
            return {"intent": "division_rankings", "division_id": division_id}
    
    # For unclear queries, use our open query parser
    open_query_result = parse_open_query(message_lower)
    if open_query_result["intent"] != "unknown":
        return open_query_result
    
    # Check if the message is just a fighter name
    fighter_id = resolve_fighter_name(message_lower)
    if fighter_id:
        return {
            "intent": "fighter_info",
            "fighter_id": fighter_id
        }
    
    # Check if the message is just a division name
    division_id = normalize_division_name(message_lower)
    if division_id:
        return {
            "intent": "division_info",
            "division_id": division_id
        }
    
    # No clear intent detected
    return {"intent": "unknown"}

@timing_decorator
def get_fighter_data(fighter_id):
    """Get fighter data with caching."""
    # Check for retired fighters
    if fighter_id and fighter_id.startswith("retired:"):
        retired_id = fighter_id.split(":", 1)[1]
        if retired_id in RETIRED_FIGHTERS:
            return RETIRED_FIGHTERS[retired_id]
        return None
    
    # Check Redis cache if enabled
    if REDIS_ENABLED:
        cache_key = f"fighter:{fighter_id}"
        cached_data = redis_client.get(cache_key)
        if cached_data:
            try:
                return json.loads(cached_data)
            except Exception as e:
                logger.error(f"Error loading fighter from Redis: {e}")
    
    # Check memory cache
    cached_data = CACHE["fighter_details"].get(fighter_id)
    if cached_data:
        return cached_data
    
    # Fetch from API
    fighter_url = f"{OCTAGON_API_BASE_URL}/fighter/{fighter_id}"
    try:
        api_rate_limiter.wait_if_needed()
        response = http_session.get(fighter_url, timeout=5)
        response.raise_for_status()
        fighter_data = response.json()
        
        # Store in memory cache
        CACHE["fighter_details"].set(fighter_id, fighter_data)
        
        # Store in Redis if enabled
        if REDIS_ENABLED:
            try:
                redis_client.setex(f"fighter:{fighter_id}", 3600, json.dumps(fighter_data))  # 1 hour TTL
            except Exception as e:
                logger.error(f"Error storing fighter in Redis: {e}")
        
        logger.debug(f"Loaded fighter data for: {fighter_id}")
        return fighter_data
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching fighter data for: {fighter_id}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching fighter data: {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching fighter data: {e}")
    except ValueError as e:
        logger.error(f"JSON parsing error for fighter data: {e}")
    
    return None

@timing_decorator
def get_division_data(division_id):
    """Get division data with caching."""
    # Check Redis cache if enabled
    if REDIS_ENABLED:
        cache_key = f"division:{division_id}"
        cached_data = redis_client.get(cache_key)
        if cached_data:
            try:
                return json.loads(cached_data)
            except Exception as e:
                logger.error(f"Error loading division from Redis: {e}")
    
    # Check memory cache
    cached_data = CACHE["division_details"].get(division_id)
    if cached_data:
        return cached_data
    
    # Fetch from API
    division_url = f"{OCTAGON_API_BASE_URL}/division/{division_id}"
    try:
        api_rate_limiter.wait_if_needed()
        response = http_session.get(division_url, timeout=5)
        response.raise_for_status()
        division_data = response.json()
        
        # Store in memory cache
        CACHE["division_details"].set(division_id, division_data)
        
        # Store in Redis if enabled
        if REDIS_ENABLED:
            try:
                redis_client.setex(f"division:{division_id}", 3600, json.dumps(division_data))  # 1 hour TTL
            except Exception as e:
                logger.error(f"Error storing division in Redis: {e}")
        
        logger.debug(f"Loaded division data for: {division_id}")
        return division_data
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching division data for: {division_id}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching division data: {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching division data: {e}")
    except ValueError as e:
        logger.error(f"JSON parsing error for division data: {e}")
    
    return None

@timing_decorator
def get_fighters_by_attribute(attribute, max_results=5, find_max=True, weight_class=None):
    """Find fighters with extreme physical attributes."""
    fighters_data = get_all_fighters_data()
    if not fighters_data:
        return []
    
    # Filter for weight class if specified
    if weight_class:
        weight_class_lower = weight_class.lower()
        fighters_data = [f for f in fighters_data if 
                         f.get("category", "").lower().startswith(weight_class_lower)]
    
    # Filter out fighters with invalid or zero values for the attribute
    valid_fighters = [f for f in fighters_data if f.get(attribute, 0) > 0]
    
    if not valid_fighters:
        return []
    
    # Sort by the attribute
    if find_max:
        sorted_fighters = sorted(valid_fighters, key=lambda x: x.get(attribute, 0), reverse=True)
    else:
        sorted_fighters = sorted(valid_fighters, key=lambda x: x.get(attribute, 0))
    
    # Return top results
    return sorted_fighters[:max_results]

@timing_decorator
def get_similar_fighters(fighter_id, limit=3):
    """Find similar fighters from the same division."""
    fighter_data = get_fighter_data(fighter_id)
    if not fighter_data or not fighter_data.get("category"):
        return []
    
    # Get division
    fighter_division = fighter_data.get("category", "").split(" ")[0].lower()
    
    # Get fighters from the same division
    load_fighters_data()
    fighters_data = CACHE["fighters"].get("fighters")
    if not fighters_data:
        return []
    
    similar_fighters = []
    for other_id, other_data in fighters_data.items():
        if other_id == fighter_id:  # Skip same fighter
            continue
            
        other_division = other_data.get("category", "").split(" ")[0].lower()
        if fighter_division == other_division:
            similar_fighters.append((other_id, other_data["name"]))
    
    # Randomly select up to 'limit' fighters to suggest
    if len(similar_fighters) > limit:
        similar_fighters = random.sample(similar_fighters, limit)
    
    return similar_fighters

def format_fighter_response(fighter_data, include_suggestions=True, fighter_id=None):
    """Format fighter information into a readable response."""
    if not fighter_data:
        return "Could not retrieve fighter information."
    
    response = [
        f"🥋 {fighter_data['name']}"
    ]
    
    if fighter_data.get('nickname'):
        response[0] += f" \"{fighter_data['nickname']}\""
    
    response.extend([
        f"📊 Record: {fighter_data.get('wins', '0')}W-{fighter_data.get('losses', '0')}L-{fighter_data.get('draws', '0')}D",
        f"🏆 Division: {fighter_data.get('category', 'Unknown')}"
    ])
    
    if fighter_data.get('fightingStyle'):
        response.append(f"🥋 Style: {fighter_data['fightingStyle']}")
    
    # Physical attributes
    physical = []
    if fighter_data.get('age'):
        physical.append(f"Age: {fighter_data['age']}")
    if fighter_data.get('height'):
        physical.append(f"Height: {fighter_data['height']}\"")
    if fighter_data.get('weight'):
        physical.append(f"Weight: {fighter_data['weight']} lbs")
    if fighter_data.get('reach'):
        physical.append(f"Reach: {fighter_data['reach']}\"")
    if fighter_data.get('legReach'):
        physical.append(f"Leg Reach: {fighter_data['legReach']}\"")
    
    if physical:
        response.append("📏 " + ", ".join(physical))
    
    # Additional information
    if fighter_data.get('placeOfBirth'):
        response.append(f"🌎 From: {fighter_data['placeOfBirth']}")
    if fighter_data.get('trainsAt'):
        response.append(f"🏋️ Trains at: {fighter_data['trainsAt']}")
    if fighter_data.get('status'):
        response.append(f"📋 Status: {fighter_data['status']}")
    if fighter_data.get('octagonDebut'):
        response.append(f"⏱️ Debut: {fighter_data['octagonDebut']}")
    if fighter_data.get('notes'):
        response.append(f"📝 {fighter_data['notes']}")
    
    # Add similar fighter suggestions if requested
    if include_suggestions and fighter_id:
        similar_fighters = get_similar_fighters(fighter_id)
        if similar_fighters:
            suggestion_text = "\n\n👀 Similar fighters you might be interested in: "
            suggestion_text += ", ".join([name for _, name in similar_fighters])
            response.append(suggestion_text)
    
    return "\n".join(response)

def format_fighter_attribute(fighter_data, attribute):
    """Format a specific attribute of a fighter."""
    if not fighter_data:
        return "Could not retrieve fighter information."
    
    fighter_name = fighter_data.get('name', 'Unknown Fighter')
    nickname = fighter_data.get('nickname', '')
    
    if nickname:
        fighter_name += f" \"{nickname}\""
    
    if attribute == "height":
        value = fighter_data.get('height', 'Unknown')
        return f"🥋 {fighter_name}'s height is {value}\""
    
    elif attribute == "weight":
        value = fighter_data.get('weight', 'Unknown')
        return f"🥋 {fighter_name}'s weight is {value} lbs"
    
    elif attribute == "reach":
        value = fighter_data.get('reach', 'Unknown')
        return f"🥋 {fighter_name}'s reach is {value}\""
    
    elif attribute == "legReach":
        value = fighter_data.get('legReach', 'Unknown')
        return f"🥋 {fighter_name}'s leg reach is {value}\""
    
    else:
        # General response with all physical attributes
        response = [f"🥋 {fighter_name} - Physical Attributes:"]
        
        physical = []
        if fighter_data.get('height'):
            physical.append(f"Height: {fighter_data['height']}\"")
        if fighter_data.get('weight'):
            physical.append(f"Weight: {fighter_data['weight']} lbs")
        if fighter_data.get('reach'):
            physical.append(f"Reach: {fighter_data['reach']}\"")
        if fighter_data.get('legReach'):
            physical.append(f"Leg Reach: {fighter_data['legReach']}\"")
        
        if physical:
            response.append("📏 " + ", ".join(physical))
        
        return "\n".join(response)

def format_physical_comparison(attribute_data):
    """Format a response comparing fighters by physical attributes."""
    attribute = attribute_data.get("attribute")
    comparison = attribute_data.get("comparison")
    
    if not attribute or not comparison:
        return "I couldn't understand which physical attribute you're asking about."
    
    find_max = comparison in ["tallest", "longest", "heaviest", "biggest"]
    
    # Try to identify a weight class from the query
    weight_class = None
    if attribute_data.get("division"):
        weight_class = attribute_data.get("division")
    
    # Get fighters with extreme values for this attribute
    top_fighters = get_fighters_by_attribute(attribute, max_results=5, find_max=find_max, weight_class=weight_class)
    
    if not top_fighters:
        return f"I couldn't find information about fighters' {attribute} at this time."
    
    # Format the response
    attribute_display_name = attribute
    if attribute == "legReach":
        attribute_display_name = "leg reach"
    
    # Add weight class to the response if specified
    weight_class_display = f" in the {weight_class}" if weight_class else " in the UFC"
    
    if find_max:
        response = [f"🏆 Fighters with the {comparison} {attribute_display_name}{weight_class_display}:"]
    else:
        response = [f"🏆 Fighters with the {comparison} {attribute_display_name}{weight_class_display}:"]
    
    for i, fighter in enumerate(top_fighters, 1):
        name = fighter.get("name", "Unknown")
        value = fighter.get(attribute, "Unknown")
        
        if attribute in ["height", "reach", "legReach"]:
            response.append(f"{i}. {name}: {value}\"")
        elif attribute == "weight":
            response.append(f"{i}. {name}: {value} lbs")
        else:
            response.append(f"{i}. {name}: {value}")
    
    return "\n".join(response)

def format_fighter_comparison(fighter1_data, fighter2_data, attribute=None):
    """Format a comparison between two fighters."""
    if not fighter1_data or not fighter2_data:
        return "Could not retrieve fighter information for comparison."
    
    fighter1_name = fighter1_data.get('name', 'Unknown Fighter 1')
    fighter2_name = fighter2_data.get('name', 'Unknown Fighter 2')
    
    # Start with a header
    response = [f"🥊 Comparing {fighter1_name} vs {fighter2_name}:"]
    
    # If a specific attribute was requested, compare just that
    if attribute:
        if attribute == "height":
            try:
                height1 = float(str(fighter1_data.get('height', '0')).replace('"', ''))
                height2 = float(str(fighter2_data.get('height', '0')).replace('"', ''))
                difference = abs(height1 - height2)
                taller = fighter1_name if height1 > height2 else fighter2_name
                shorter = fighter2_name if height1 > height2 else fighter1_name
                
                response.append(f"📏 {fighter1_name}: {fighter1_data.get('height', 'Unknown')}\"")
                response.append(f"📏 {fighter2_name}: {fighter2_data.get('height', 'Unknown')}\"")
                response.append(f"Result: {taller} is {difference:.1f}\" taller than {shorter}")
            except (ValueError, TypeError):
                response.append(f"📏 {fighter1_name}: {fighter1_data.get('height', 'Unknown')}\"")
                response.append(f"📏 {fighter2_name}: {fighter2_data.get('height', 'Unknown')}\"")
                
        elif attribute == "weight":
            try:
                weight1 = float(fighter1_data.get('weight', '0'))
                weight2 = float(fighter2_data.get('weight', '0'))
                difference = abs(weight1 - weight2)
                heavier = fighter1_name if weight1 > weight2 else fighter2_name
                lighter = fighter2_name if weight1 > weight2 else fighter1_name
                
                response.append(f"⚖️ {fighter1_name}: {fighter1_data.get('weight', 'Unknown')} lbs")
                response.append(f"⚖️ {fighter2_name}: {fighter2_data.get('weight', 'Unknown')} lbs")
                response.append(f"Result: {heavier} is {difference:.1f} lbs heavier than {lighter}")
            except (ValueError, TypeError):
                response.append(f"⚖️ {fighter1_name}: {fighter1_data.get('weight', 'Unknown')} lbs")
                response.append(f"⚖️ {fighter2_name}: {fighter2_data.get('weight', 'Unknown')} lbs")
                
        elif attribute == "reach":
            try:
                reach1 = float(str(fighter1_data.get('reach', '0')).replace('"', ''))
                reach2 = float(str(fighter2_data.get('reach', '0')).replace('"', ''))
                difference = abs(reach1 - reach2)
                longer = fighter1_name if reach1 > reach2 else fighter2_name
                shorter = fighter2_name if reach1 > reach2 else fighter1_name
                
                response.append(f"👐 {fighter1_name}: {fighter1_data.get('reach', 'Unknown')}\"")
                response.append(f"👐 {fighter2_name}: {fighter2_data.get('reach', 'Unknown')}\"")
                response.append(f"Result: {longer} has a {difference:.1f}\" longer reach than {shorter}")
            except (ValueError, TypeError):
                response.append(f"👐 {fighter1_name}: {fighter1_data.get('reach', 'Unknown')}\"")
                response.append(f"👐 {fighter2_name}: {fighter2_data.get('reach', 'Unknown')}\"")
        
        elif attribute == "legReach":
            try:
                legreach1 = float(str(fighter1_data.get('legReach', '0')).replace('"', ''))
                legreach2 = float(str(fighter2_data.get('legReach', '0')).replace('"', ''))
                difference = abs(legreach1 - legreach2)
                longer = fighter1_name if legreach1 > legreach2 else fighter2_name
                shorter = fighter2_name if legreach1 > legreach2 else fighter1_name
                
                response.append(f"🦵 {fighter1_name}: {fighter1_data.get('legReach', 'Unknown')}\"")
                response.append(f"🦵 {fighter2_name}: {fighter2_data.get('legReach', 'Unknown')}\"")
                response.append(f"Result: {longer} has a {difference:.1f}\" longer leg reach than {shorter}")
            except (ValueError, TypeError):
                response.append(f"🦵 {fighter1_name}: {fighter1_data.get('legReach', 'Unknown')}\"")
                response.append(f"🦵 {fighter2_name}: {fighter2_data.get('legReach', 'Unknown')}\"")
    
    else:
        # Compare all relevant attributes
        response.append("\n📊 Record:")
        response.append(f"{fighter1_name}: {fighter1_data.get('wins', '0')}W-{fighter1_data.get('losses', '0')}L-{fighter1_data.get('draws', '0')}D")
        response.append(f"{fighter2_name}: {fighter2_data.get('wins', '0')}W-{fighter2_data.get('losses', '0')}L-{fighter2_data.get('draws', '0')}D")
        
        response.append("\n📏 Physical Attributes:")
        response.append(f"{fighter1_name}: Height {fighter1_data.get('height', 'Unknown')}\", Weight {fighter1_data.get('weight', 'Unknown')} lbs")
        response.append(f"{fighter2_name}: Height {fighter2_data.get('height', 'Unknown')}\", Weight {fighter2_data.get('weight', 'Unknown')} lbs")
        
        response.append("\n👐 Reach:")
        response.append(f"{fighter1_name}: Arm Reach {fighter1_data.get('reach', 'Unknown')}\", Leg Reach {fighter1_data.get('legReach', 'Unknown')}\"")
        response.append(f"{fighter2_name}: Arm Reach {fighter2_data.get('reach', 'Unknown')}\", Leg Reach {fighter2_data.get('legReach', 'Unknown')}\"")
        
        # Fighting styles
        response.append("\n🥋 Fighting Styles:")
        response.append(f"{fighter1_name}: {fighter1_data.get('fightingStyle', 'Unknown')}")
        response.append(f"{fighter2_name}: {fighter2_data.get('fightingStyle', 'Unknown')}")
    
    return "\n".join(response)

def format_champion_response(division_data, champion_data):
    """Format champion information into a readable response."""
    if not division_data or not division_data.get('champion'):
        return "Could not retrieve champion information."
    
    response = [
        f"🏆 The current {division_data['categoryName']} Champion is:",
        f"👑 {division_data['champion']['championName']}"
    ]
    
    if champion_data:
        if champion_data.get('nickname'):
            response[1] += f" \"{champion_data['nickname']}\""
        
        response.extend([
            f"📊 Record: {champion_data.get('wins', '0')}W-{champion_data.get('losses', '0')}L-{champion_data.get('draws', '0')}D"
        ])
        
        if champion_data.get('fightingStyle'):
            response.append(f"🥋 Style: {champion_data['fightingStyle']}")
        
        # Add physical attributes
        physical = []
        if champion_data.get('height'):
            physical.append(f"Height: {champion_data['height']}\"")
        if champion_data.get('weight'):
            physical.append(f"Weight: {champion_data['weight']} lbs")
        if champion_data.get('reach'):
            physical.append(f"Reach: {champion_data['reach']}\"")
            
        if physical:
            response.append("📏 " + ", ".join(physical))
    
    # Add top contender if available
    if division_data.get('fighters') and len(division_data['fighters']) > 0:
        response.append(f"🥈 #1 Contender: {division_data['fighters'][0]['name']}")
    
    return "\n".join(response)

def format_rankings_response(division_data):
    """Format division rankings into a readable response."""
    if not division_data:
        return "Could not retrieve rankings information."
    
    response = [f"🏆 {division_data['categoryName']} Rankings:"]
    
    if division_data.get('champion'):
        response.append(f"👑 Champion: {division_data['champion']['championName']}")
    
    if division_data.get('fighters'):
        response.append("\n🥇 Top Contenders:")
        for i, fighter in enumerate(division_data['fighters'][:10], 1):
            response.append(f"{i}. {fighter['name']}")
    
    return "\n".join(response)

def format_all_champions_response():
    """Format a response with all current champions."""
    load_rankings_data()
    rankings_data = CACHE["rankings"].get("rankings")
    if not rankings_data:
        return "Could not retrieve champions information."
    
    response = ["👑 Current UFC Champions:"]
    
    for rank in rankings_data:
        if "champion" in rank and rank["champion"]:
            response.append(f"🏆 {rank['categoryName']}: {rank['champion']['championName']}")
    
    return "\n".join(response)

def format_all_rankings_response():
    """Format a response prompting for which division rankings to show."""
    return "Which division's rankings would you like to see? Options include:\n\n" + \
           "- Flyweight\n" + \
           "- Bantamweight\n" + \
           "- Featherweight\n" + \
           "- Lightweight\n" + \
           "- Welterweight\n" + \
           "- Middleweight\n" + \
           "- Light Heavyweight\n" + \
           "- Heavyweight\n" + \
           "- Men's Pound-for-Pound\n" + \
           "- Women's Strawweight\n" + \
           "- Women's Flyweight\n" + \
           "- Women's Bantamweight\n" + \
           "- Women's Pound-for-Pound"

@timing_decorator
def generate_response(intent_data):
    """Generate a response based on the intent and entities."""
    intent = intent_data.get("intent")
    
    if intent == "fighter_info":
        fighter_id = intent_data.get("fighter_id")
        fighter_data = get_fighter_data(fighter_id)
        return format_fighter_response(fighter_data, include_suggestions=True, fighter_id=fighter_id)
    
    elif intent == "fighter_attribute":
        fighter_id = intent_data.get("fighter_id")
        attribute = intent_data.get("attribute")
        fighter_data = get_fighter_data(fighter_id)
        return format_fighter_attribute(fighter_data, attribute)
    
    elif intent == "fighter_comparison":
        fighter1_id = intent_data.get("fighter1_id")
        fighter2_id = intent_data.get("fighter2_id")
        attribute = intent_data.get("attribute")
        
        fighter1_data = get_fighter_data(fighter1_id)
        fighter2_data = get_fighter_data(fighter2_id)
        
        return format_fighter_comparison(fighter1_data, fighter2_data, attribute)
    
    elif intent == "physical_comparison":
        attribute_data = intent_data.get("attribute_data")
        return format_physical_comparison(attribute_data)
    
    elif intent == "division_champion":
        division_id = intent_data.get("division_id")
        division_data = get_division_data(division_id)
        
        if division_data and division_data.get("champion"):
            champion_id = division_data["champion"]["id"]
            champion_data = get_fighter_data(champion_id)
            return format_champion_response(division_data, champion_data)
        else:
            return f"I couldn't find champion information for that division."
    
    elif intent == "division_rankings":
        division_id = intent_data.get("division_id")
        division_data = get_division_data(division_id)
        return format_rankings_response(division_data)
    
    elif intent == "division_info":
        division_id = intent_data.get("division_id")
        division_data = get_division_data(division_id)
        return format_rankings_response(division_data)
    
    elif intent == "all_champions":
        return format_all_champions_response()
    
    elif intent == "all_rankings":
        return format_all_rankings_response()
    
    elif intent == "general_mma_question":
        return (
            "I can help you with UFC fighter information. Here are some things you can ask me about:\n\n"
            "👤 Fighter Information: 'Who is Jon Jones?' or 'Tell me about Ngannou'\n"
            "🏆 Champions: 'Who is the lightweight champion?' or 'Show all champions'\n"
            "📊 Rankings: 'Show me the heavyweight rankings' or 'Top 10 bantamweights'\n"
            "👊 Comparisons: 'Who is taller, Jones or Pereira?' or 'Compare Makhachev vs Volkanovski'\n"
            "📏 Physical Stats: 'Who has the longest reach?' or 'How tall is Alex Pereira?'\n\n"
            "Try asking one of these questions or be more specific about what you'd like to know!"
        )
    
    else:
        return (
            "I'm not sure what you're asking about. I can provide information about UFC fighters, "
            "champions, rankings, and physical attributes. Try asking something like:\n\n"
            "- 'Who is Islam Makhachev?'\n"
            "- 'Show me the lightweight rankings'\n"
            "- 'Who is the heavyweight champion?'\n"
            "- 'Who has the longest reach in the UFC?'\n"
            "- 'Compare Jones vs Pereira'"
        )

def start_background_refresh():
    """Start a background thread to periodically refresh data."""
    def refresh_job():
        while True:
            try:
                logger.info("Starting background data refresh")
                load_fighters_data()
                load_rankings_data()
                logger.info("Background data refresh completed")
                
                # Sleep for 30 minutes before refreshing again
                time.sleep(1800)
            except Exception as e:
                logger.error(f"Error in background refresh: {e}")
                time.sleep(300)  # On error, retry after 5 minutes
    
    # Start the background thread
    refresh_thread = threading.Thread(target=refresh_job, daemon=True)
    refresh_thread.start()
    logger.info("Background refresh thread started")

@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "MMA Webhook is running!"})

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()

    # If there's no data or message, return an error
    if not data or "message" not in data:
        return jsonify({"response": "Invalid request, missing 'message' in JSON data"}), 400

    # Sanitize input
    user_message = sanitize_input(data.get("message", "")).strip()
    logger.debug(f"Received data: {data}")
    logger.debug(f"User message: {user_message}")

    # Parse intent from user message
    intent_data = parse_query_intent(user_message)
    logger.debug(f"Parsed intent: {intent_data}")
    
    # Generate response based on intent
    response = generate_response(intent_data)
    logger.debug(f"Generated response: {response}")
    
    return jsonify({"response": response})

@app.route("/test", methods=["GET"])
def test():
    """Simple test endpoint to verify the server is running."""
    return jsonify({"status": "success", "message": "MMA Webhook is operational"})

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint for monitoring."""
    # Check API connectivity
    try:
        api_test = http_session.get(f"{OCTAGON_API_BASE_URL}/test", timeout=2)
        api_status = api_test.status_code == 200
    except:
        api_status = False
    
    # Check cache status
    cache_status = bool(CACHE["fighters"].get("fighters"))
    
    # Check Redis if enabled
    redis_status = False
    if REDIS_ENABLED:
        try:
            redis_status = redis_client.ping()
        except:
            redis_status = False
    
    # Overall health status
    is_healthy = api_status and cache_status
    
    status_code = 200 if is_healthy else 503
    
    return jsonify({
        "status": "healthy" if is_healthy else "unhealthy",
        "api_connected": api_status,
        "cache_loaded": cache_status,
        "redis_connected": redis_status if REDIS_ENABLED else "disabled",
        "timestamp": time.time()
    }), status_code

@app.route("/clear-cache", methods=["POST"])
def clear_cache():
    """Admin endpoint to clear caches."""
    # Check for simple auth via header
    auth_header = request.headers.get("X-Admin-Key", "")
    if auth_header != "your-secret-admin-key":  # Replace with real secret in production
        return jsonify({"error": "Unauthorized"}), 401
    
    # Clear memory caches
    for cache_key in CACHE:
        if isinstance(CACHE[cache_key], TTLCache):
            CACHE[cache_key].clear()
        else:
            CACHE[cache_key] = None
    
    # Clear Redis cache if enabled
    if REDIS_ENABLED:
        try:
            redis_client.flushdb()
        except Exception as e:
            logger.error(f"Error clearing Redis: {e}")
    
    return jsonify({"status": "success", "message": "All caches cleared"})

if __name__ == "__main__":
    # Preload common data on startup
    load_fighters_data()
    load_rankings_data()
    build_division_mapping()
    
    # Start background refresh
    start_background_refresh()
    
    app.run(port=5000, debug=True)
if __name__ == "__main__":
    # Preload common data on startup
    load_fighters_data()
    load_rankings_data()
    build_division_mapping()
    
    # Start background refresh
    start_background_refresh()
    
    # Modified to work with Render - use PORT environment variable if available
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)