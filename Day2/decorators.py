import argparse



def log_time(func):
    print("I am here in log_time")
    def wrapper(*args, **kwargs):
        print(*args)
        print(*kwargs)
        print("I am from log_time => Wrapper")
        result = func(*args,**kwargs)
        return result
    return wrapper

@log_time
def process():
    print("I am from process method")

process()

# def sample():
#     return "Hello"

# log_time(sample)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some data..")
    parser.add_argument("--name", type=str)
    parser.add_argument("--age", type=int)

    args = parser.parse_args()
    print(args.name)
    print(args.age)
    process()


        