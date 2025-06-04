from src.lambda_handler import lambda_handler

def main():
    
    # Mock event and context
    event = {}
    context = None
    
    result = lambda_handler(event, context)
    print(result)


if __name__ == "__main__":
    main()
