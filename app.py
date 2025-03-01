from flask import Flask, request, jsonify
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    logger.info("Home endpoint accessed")
    return "MMA Webhook is running!"

@app.route('/webhook', methods=['POST'])
def webhook():
    logger.info("Webhook endpoint accessed")
    data = request.json
    # Process your MMA webhook data here
    logger.info(f"Received webhook data: {data}")
    
    # Return a response
    return jsonify({"status": "success", "message": "Webhook received"})

if __name__ == '__main__':
    logger.info("Starting Flask application")
    print("MMA Webhook starting on http://127.0.0.1:5000")
    app.run(debug=True)