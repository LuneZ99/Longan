import time


def get_ms():
    return round(time.time() * 1000)


if __name__ == '__main__':
    print(get_ms())
