import os
import base64
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

base64_image = encode_image("images/1.jpg")  # 프로젝트 안의 이미지 파일 경로

client = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

chat_completion = client.chat.completions.create(
    model="meta-llama/llama-4-scout-17b-16e-instruct",
    messages=[
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "이 사진 내용 한국어로 요약해줘."
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{base64_image}"
                    },
                },
            ],
        }
    ],
    max_completion_tokens=1024,
)

print(chat_completion.choices[0].message.content)