import base64
from client.client_manager import qwen_client


client = qwen_client

def qwen_vl_call(image_path, prompt=None):
    """
    调用通义千问VL模型进行图像理解
    Args:
        image_path: 图片路径
        prompt: 提示词,默认为简单描述图片内容
    Returns:
        模型回复的文本内容和费用信息
    """
    if prompt is None:
        prompt = "你是一个研报专家，从研报的图表和图片提取有用信息。请直接从图片提取有效信息，并且直接返回内容，避免多余，无用信息的描述。比如“图片中的有效信息如下：”"

    # base64编码图片
    def encode_image(image_path):
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    base64_image = encode_image(image_path)

    retry_count = 0
    max_retries = 3
    while retry_count < max_retries:
        try:
            completion = client.chat.completions.create(
                model="qwen-vl-max-latest",
                messages=[
                    {
                        "role": "system",
                        "content": [{"type":"text","text": "You are a helpful assistant."}]
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ]
            )
            
            response_text = completion.choices[0].message.content
            print("大模型的回复:\n", response_text)
            
            # 计算费用
            input_tokens = completion.usage.prompt_tokens
            output_tokens = completion.usage.completion_tokens
            input_cost = input_tokens * 0.003 / 1000  # 输入费用
            output_cost = output_tokens * 0.009 / 1000  # 输出费用
            total_cost = input_cost + output_cost
            
            return {
                "response": response_text,
                "cost": total_cost
            }
            
        except Exception as e:
            retry_count += 1
            if retry_count == max_retries:
                print(f"调用模型失败,已重试{max_retries}次,错误信息: {str(e)}")
                raise e
            print(f"调用模型失败,正在进行第{retry_count}次重试...")