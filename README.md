Yet another DataFrame library, this time with a focus on simplicity and usability and not on performance (just like a koala: cute and slow).

In particular, it offers a limited, simple and intuitive set of operations for manipulation and inspection of tabular data, with virtually no learning curve.

The below is an extract from the carried example in `example.ipynb`:

```python
from koalas import DataFrame
from example import chess

# Fetches results of chess games played on chess.com for the given player and period
df = DataFrame.from_records(chess('TineBerger', 2023, 9))

# Identifies which openings are most and least successful when playing as black.
def summary(df):
    return (
        df
            .filter('Color', 'Black')
            .apply('Opening', lambda moves: ' '.join(moves[:1]), 'Moves')
            .group('Opening')
            .apply('Count', len, 'Outcome')
            .apply('Win Rate', lambda outcomes: outcomes.count('Win') / len(outcomes), 'Outcome')                
            .apply('Win Percentage', lambda rate: round(100. * rate, 2), 'Win Rate')
            .sort('Win Percentage')
            .reverse()
            .apply('Minimum', lambda count: count > 20, 'Count')
            .filter('Minimum', True)
            .select('Opening', 'Win Percentage')
    )

summary(df)

# Opening Win Percentage
# ------- --------------
# 1. e3   50.0          
# 1. e4   48.58         
# 1. d4   45.6          
# 1. Nf3  39.13         
# 1. c4   36.17      

# Compare opening performance against another player
left = df
right = DataFrame.from_records(chess('architecturalpain', 2023, 9))

left = summary(left).rename('Win Percentage', 'Win Percentage (Left)')
right = summary(right).rename('Win Percentage', 'Win Percentage (Right)')
comparison = (
    left
        .join(right, 'Opening')
        .apply('Difference', lambda a, b: round(b - a, 2), 'Win Percentage (Left)', 'Win Percentage (Right)')        
        .export('csv', 'performance.csv') # will export to a .csv file
        .sort('Difference')
        .reverse()
)
comparison

# Opening Win Percentage (Left) Win Percentage (Right) Difference
# ------- --------------------- ---------------------- ----------
# 1. d4   45.6                  51.4                   5.8       
# 1. e4   48.58                 50.2                   1.62      
```