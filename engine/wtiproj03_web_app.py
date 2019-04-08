import cherrypy
from wtiproj01_client import RatingsClient

class RecomendationsApp(object):

    cli = RatingsClient()

    @cherrypy.expose
    def index(self):
        return "Hello World!"

    @cherrypy.expose
    def ratings(self):
        return self.cli.get_ratings_json()


cherrypy.quickstart(RecomendationsApp())