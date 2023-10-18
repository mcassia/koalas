Yet another DataFrame library, this time with focus on simplicity and usability and not on performance (just like a koala: cute and slow).

It offers a limited yet complete set of simple and intutive operations for manipulation and inspection of tabular data, with virtually no learning curve.

It tries to be as Pythonic as possible and has a penchant for method chaining.

# Examples

The following are carried examples, implemented in Jupyter notebooks under `examples/`.

## COVID Cases in Europe

The below an example of how raw data on Covid cases in Europe can be summarised to identify the most impacted countries by week:

```python
df = get_covid_data()

df[1000:1003]

# country country_code year_week level    region region_name new_cases tests_done population testing_rate positivity_rate testing_data_source
# ------- ------------ --------- -----    ------ ----------- --------- ---------- ---------- ------------ --------------- -------------------
# Czechia CZ           2020-W16  national CZ     Czechia     755.0     44165.0    10516707.0 419.9508458  1.709498472     TESSy COVID-19     
# Czechia CZ           2020-W17  national CZ     Czechia     658.0     46583.0    10516707.0 442.9428337  1.412532469     TESSy COVID-19     
# Czechia CZ           2020-W18  national CZ     Czechia     379.0     43476.0    10516707.0 413.3993654  0.871745331     TESSy COVID-19     


summary = (
    df    
        .apply('New Cases', lambda new_cases: new_cases or 0., 'new_cases')
        .rename('country', 'Country')
        .rename('year_week', 'Period')        
        .rename('population', 'Population')
        .select('Country', 'New Cases', 'Period', 'Population')      
        .apply('New Cases (Norm.)', lambda new_cases, population: round(100_000 * new_cases / population, 2), 'New Cases', 'Population')  
        .group('Period')
        .apply('Most New Cases (Norm.)', lambda new_cases: max(new_cases), 'New Cases (Norm.)')
        .apply(
            'Country with Most New Cases (Norm.)',
            lambda countries, new_cases, most_new_cases: [c for c, n in zip(countries, new_cases) if n == most_new_cases][0],
            'Country', 'New Cases (Norm.)', 'Most New Cases (Norm.)'
        )
        .select('Period', 'Most New Cases (Norm.)', 'Country with Most New Cases (Norm.)')
        [:20]
)

summary

# Period   Most New Cases (Norm.) Country with Most New Cases (Norm.)
# ------   ---------------------- -----------------------------------
# 2020-W01 0.17                   Denmark                            
# 2020-W02 0.2                    Denmark                            
# 2020-W03 0.14                   Denmark                            
# 2020-W04 0.26                   Denmark                            
# 2020-W05 0.22                   Denmark                            
# 2020-W06 3.99                   Iceland                            
# 2020-W07 0.24                   Denmark                            
# 2020-W08 0.29                   Italy                              
# 2020-W09 3.35                   Italy                              
# 2020-W10 14.13                  Italy                              
# 2020-W11 35.76                  Italy                              
# 2020-W12 122.53                 Iceland                            
# 2020-W13 418.23                 Estonia                            
# 2020-W14 116.94                 Iceland                            
# 2020-W15 96.19                  Belgium                            
# 2020-W16 98.24                  Ireland                            
# 2020-W17 77.08                  Ireland                            
# 2020-W18 46.23                  Ireland                            
# 2020-W19 39.73                  Sweden                             
# 2020-W20 37.02                  Sweden    
```

## Chess Performance

The below showcases how raw data on chess games can be manipulated to identify weak openings.

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