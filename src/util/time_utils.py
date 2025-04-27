from datetime import timedelta


def seconds_to_hours(seconds):
    return str(
        timedelta(seconds=seconds)
    )
