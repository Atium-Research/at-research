import dataframely as dy

class SignalSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()
    name = dy.String()
    signal = dy.Float()

class ScoreSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()
    name = dy.String()
    score = dy.Float()

class AlphaSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()
    name = dy.String()
    alpha = dy.Float()

class UniverseSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()