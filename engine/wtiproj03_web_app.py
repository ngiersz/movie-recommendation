import cherrypy
from engine.api_logic import RatingsClient


class RecomendationsApp(object):

    cli = RatingsClient()

    @cherrypy.expose
    def index(self):
        return "Hello World!"

    @cherrypy.expose
    def ratings(self):
        return self.cli.get_ratings_json()


cherrypy.quickstart(RecomendationsApp())
