import time
from datetime import datetime, timedelta


def get_ms():
    return round(time.time() * 1000)


def get_timestamp_list_hour(hours, n=99):
    now = datetime.now()
    timestamp_last = now.replace(hour=now.hour // hours * hours, minute=0, second=0, microsecond=0)
    timestamp_list = [int((timestamp_last - timedelta(hours=i * hours)).timestamp() * 1000) for i in range(1, n + 1)][
                     ::-1]
    return timestamp_list


def split_list_averagely(lst, n):
    avg_len = len(lst) // n
    remainder = len(lst) % n
    result = []
    start = 0
    for i in range(n):
        end = start + avg_len + (i < remainder)
        result.append(lst[start:end])
        start = end
    return result


def split_list_by_length(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]


if __name__ == '__main__':
    print(get_ms())
