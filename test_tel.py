import requests
import json

# Load your config to get credentials
with open('config/config.json', 'r') as f:
    config = json.load(f)

token = config['telegram']['token']
chat_id = config['telegram']['chat_id']

def test_message():
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "✅ Jain Family Office: Telegram Connection Successful!",
        "parse_mode": "Markdown"
    }
    
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print("Success! Check your Telegram.")
    else:
        print(f"Failed: {response.text}")

if __name__ == "__main__":
    test_message()