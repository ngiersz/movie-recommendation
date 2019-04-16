import redis


def add_to_queue(name, object):
    server = redis.StrictRedis(host='localhost', port=6381, db=0)
    server.rpush(name, object)


def flush_queue(name):
    server = redis.StrictRedis(host='localhost', port=6381, db=0)
    whole_queue_batch = server.lrange(name, 0, -1)
    server.ltrim(name, len(whole_queue_batch), -1)


def get_queue(name):
    server = redis.StrictRedis(host='localhost', port=6381, db=0)
    return server.lrange(name, 0, -1)


def flush_db():
    server = redis.StrictRedis(host='localhost', port=6381, db=0)
    server.flushdb()

