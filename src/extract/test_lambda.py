from lambda_handlers.test_handler import load_config

if __name__ == "__main__":
    config = load_config()
    print("Config content:", config)
