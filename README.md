Yet another DataFrame library, this time with a focus on simplicity and usability and not on performance (just like a koala: cute and slow).

In particular, it offers a limited, simple and intuitive set of operations for manipulation and inspection of tabular data, with virtually no learning curve.

The below is an extract from the carried example in `example.ipynb`:

```python
from koalas import DataFrame
from example import chess

# Fetches results of chess games played on chess.com for the given player and period
df = DataFrame.from_records(list(chess('TineBerger', 2023, 10)))

# Identifies which openings are most and least successful when playing as white.
summary = (
    df
        .filter('Color', 'White')
        .apply('Moves', lambda moves: ' '.join(moves[:4]))
        .group('Moves')
        .apply('Outcome', len, 'Count')
        .apply('Outcome', lambda o: o.count('Win') / (o.count('Win') + o.count('Draw') + o.count('Lose')))                
        .rename('Outcome', 'Win Rate')
        .apply('Win Rate', lambda rate: round(100. * rate, 2))
        .sort('Win Rate', False)
        .apply('Count', lambda count: count > 25, 'Minimum')
        .filter('Minimum', True)
        .select('Moves', 'Win Rate', 'Count')
)
```

the resulting `summary` is then represented as:

```
Moves                          Win Rate Count
-----                          -------- -----
1. Nc3 1... Nf6 2. e4 2... d6  67.44    43   
1. Nc3 1... d5 2. e4 2... d4   58.96    173  
1. Nc3 1... Nf6 2. e4 2... e5  55.74    61   
1. Nc3 1... Nf6 2. e4 2... g6  54.84    31   
1. Nc3 1... e5 2. e4 2... Nf6  51.79    56   
1. Nc3 1... g6 2. e4 2... Bg7  50.0     56   
1. Nc3 1... c6 2. e4 2... d5   48.15    54   
1. Nc3 1... d5 2. e4 2... dxe4 47.54    61   
1. Nc3 1... c5 2. e4 2... Nc6  43.18    44   
1. Nc3 1... e6 2. e4 2... d5   39.47    38   
1. Nc3 1... e5 2. e4 2... Nc6  34.55    55   
```
