import dataframely as dy

class SignalSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()
    name = dy.String()
    signal = dy.Float(nullable=True)

class ScoreSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()
    name = dy.String()
    score = dy.Float(nullable=True)

class AlphaSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()
    name = dy.String()
    alpha = dy.Float(nullable=True)

class UniverseSchema(dy.Schema):
    date = dy.Date()
    ticker = dy.String()