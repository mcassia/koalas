Yet another DataFrame library, this time with a focus on simplicity and usability and not on performance (just like a koala: cute and slow).

In particular, it offers a limited, simple and intuitive set of operations for manipulation and inspection of tabular data, with virtually no learning curve.

```
from koalas import DataFrame

df = DataFrame.from_records(
    [
        dict(Name='Gustavo', Points=17, Country='Brazil', Completed=False),
        dict(Name='Wojciech', Points=14, Country='Poland', Complete=True),
        dict(Name='James', Points=11, Country='United Kingdom', Completed=True),
        dict(Name='Tomer', Points=29, Country='United Kingdom', Completed=True),
    ]
)

results = (
    df
        .filter('Completed', True)
        .group('Country')
        .apply('Points', sum)
        .select('Country', 'Points')
        .sort('Points', False)
        .to_json()
)
```
