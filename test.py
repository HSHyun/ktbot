import os

from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

chat_completion = client.chat.completions.create(
    messages=[
        {
            "role": "user",
            "content": "GROQ 에서 free tier에서 이미지를 읽고 텍스트 추출해서 요약할 수 있는 모델 있어?",
        }
    ],
    model="meta-llama/llama-4-maverick-17b-128e-instruct",
)

print(chat_completion.choices[0].message.content)