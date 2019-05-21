import requests


def send_request_and_print(method, url, data={}):
    r = requests.request(method=method, url=url, data=data)
    print('request.url: ' + r.url)
    print('request.status_code: ' + str(r.status_code))
    print('request.headers: ' + str(r.headers))
    try:
        print('request.content: ' + str(r.json()))
    except:
        print('request.content: ')
    print('*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-')


new1 = '{"genre_Action": 0, "genre_Adventure": 1, "genre_Animation": 0, "genre_Children": 0, "genre_Comedy": 1, "genre_Crime": 0, "genre_Documentary": 0, "genre_Drama": 0, "genre_Fantasy": 0, "genre_Film-Noir": 0, "genre_Horror": 0, "genre_Musical": 0, "genre_Mystery": 0, "genre_Romance": 1, "genre_Sci-Fi": 0, "genre_Thriller": 0, "genre_War": 0, "genre_Western": 0, "movieID": 2222, "rating": 1, "userID": 80}'
new2 = '{"genre_Action": 1, "genre_Adventure": 0, "genre_Animation": 0, "genre_Children": 0, "genre_Comedy": 1, "genre_Crime": 0, "genre_Documentary": 0, "genre_Drama": 0, "genre_Fantasy": 0, "genre_Film-Noir": 0, "genre_Horror": 0, "genre_Musical": 1, "genre_Mystery": 0, "genre_Romance": 0, "genre_Sci-Fi": 0, "genre_Thriller": 0, "genre_War": 1, "genre_Western": 0, "movieID": 7222, "rating": 3, "userID": 82}'
new3 = '{"genre_Action": 0, "genre_Adventure": 1, "genre_Animation": 0, "genre_Children": 1, "genre_Comedy": 0, "genre_Crime": 0, "genre_Documentary": 0, "genre_Drama": 0, "genre_Fantasy": 0, "genre_Film-Noir": 0, "genre_Horror": 0, "genre_Musical": 0, "genre_Mystery": 0, "genre_Romance": 1, "genre_Sci-Fi": 0, "genre_Thriller": 0, "genre_War": 0, "genre_Western": 0, "movieID": 2235, "rating": 5, "userID": 81}'

send_request_and_print('GET', 'http://127.0.0.1:5000/ratings')
send_request_and_print('GET', 'http://127.0.0.1:5000/avg-genre-ratings/all-users')
send_request_and_print('DELETE', 'http://127.0.0.1:5000/ratings')
send_request_and_print('GET', 'http://127.0.0.1:5000/ratings')

send_request_and_print('POST', 'http://127.0.0.1:5000/rating', new1)
send_request_and_print('POST', 'http://127.0.0.1:5000/rating', new2)
send_request_and_print('POST', 'http://127.0.0.1:5000/rating', new3)
send_request_and_print('GET', 'http://127.0.0.1:5000/ratings')

send_request_and_print('GET', 'http://127.0.0.1:5000/avg-genre-ratings/all-users')
send_request_and_print('GET', 'http://127.0.0.1:5000/avg-genre-ratings?user=80')


