import redis


def add_to_queue(name, object):
    server = redis.StrictRedis(host='localhost', port=6379, db=0)
    server.rpush(name, object)


def flush_queue(name):
    server = redis.StrictRedis()
    whole_queue_batch = server.lrange(name, 0, -1)
    server.ltrim(name, len(whole_queue_batch), -1)
    # server.ltrim('queue1', server.llen('queue1') + 1, server.llen('queue1') + 2)


def get_queue(name):
    server = redis.StrictRedis()
    return server.lrange(name, 0, -1)


def flush_db():
    server = redis.StrictRedis()
    server.flushdb()
