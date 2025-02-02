# from tasks import run

# if __name__ == "__main__":
#     run()


import argparse
from tasks import run as external_mqtt
from tasksinternal import run as internal_mqtt
from tasksawt200 import run as awt200mqtt

def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Run the script with arguments for IP, username, password, and topic.")

    # Add arguments
    parser.add_argument("--path", default="external",required=True, help="External or internal mqtt connection")

    # Parse the arguments
    args = parser.parse_args()

    # Pass arguments to your task
    if args.path == "external":
        external_mqtt()
    elif args.path == "awt200":
        awt200mqtt()
    else:
        internal_mqtt()




if __name__ == "__main__":
    main()