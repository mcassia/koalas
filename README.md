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
        .apply('Opening', lambda moves: ' '.join(moves[:4]), 'Moves')
        .group('Opening')
        .apply('Count', len, 'Outcome')
        .apply('Win Rate', lambda outcomes: outcomes.count('Win') / len(outcomes), 'Outcome')                
        .apply('Win Percentage', lambda rate: round(100. * rate, 2), 'Win Rate')
        .sort('Win Percentage', False)
        .apply('Minimum', lambda count: count > 25, 'Count')
        .filter('Minimum', True)
        .select('Opening', 'Win Percentage', 'Count')
)
```

the resulting `summary` is then represented as:

```
Opening                        Win Percentage Count
-------                        -------------- -----
1. Nc3 1... Nf6 2. e4 2... d6  64.58          48   
1. Nc3 1... d5 2. e4 2... d4   58.01          181  
1. Nc3 1... Nf6 2. e4 2... e5  57.58          66   
1. Nc3 1... Nf6 2. e4 2... g6  53.12          32   
1. Nc3 1... g6 2. e4 2... Bg7  50.85          59   
1. Nc3 1... d5 2. e4 2... dxe4 50.0           64   
1. Nc3 1... e5 2. e4 2... Nf6  50.0           62   
1. Nc3 1... c6 2. e4 2... d5   45.61          57   
1. Nc3 1... c5 2. e4 2... Nc6  44.68          47   
1. Nc3 1... e6 2. e4 2... d5   39.47          38   
1. Nc3 1... e5 2. e4 2... Nc6  36.21          58   
```
