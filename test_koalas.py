from unittest import TestCase
from koalas import DataFrame, DataFrameException


class DataFrameTests(TestCase):

    def test_filter(self):
        records = [
            dict(Name='John', Profession='Architect'),
            dict(Name='Peter', Profession='Architect'),
            dict(Name='Jane', Profession='Journalist'),
        ]
        self.assertEqual(
            DataFrame.from_records(records).filter(
                'Profession', 'Architect').to_records(),
            records[:2]
        )
        self.assertEqual(
            DataFrame.from_records(records).filter(
                'Profession', 'Journalist').to_records(),
            records[2:]
        )
        self.assertEqual(
            DataFrame.from_records(records).filter(
                'Profession', 'Engineer').to_records(),
            []
        )
        with self.assertRaises(DataFrameException):
            _ = DataFrame.from_records(records).filter('Age', 42)

    def test_rename(self):
        df = DataFrame.from_records([dict(Name='Jane Doe', Age=42)])
        self.assertEqual(
            df.rename('Name', 'FullName').to_records(),
            [dict(FullName='Jane Doe', Age=42)]
        )
        with self.assertRaises(DataFrameException):
            _ = df.rename('Profession', 'Job')

    def test_join(self):
        left = DataFrame.from_records(
            [
                dict(Name='John', Company='AnyCompany', Age=42),
                dict(Name='Jane', Company='AnyCompany', Age=37),
                dict(Name='Jean', Company='SomeCompany', Age=23),
                dict(Name='Jack', Company='YetAnotherCompany', Age=28),
            ]
        )
        right = DataFrame.from_records(
            [
                dict(Company='AnyCompany', Sector='Finance'),
                dict(Company='SomeCompany', Sector='Consulting'),
                dict(Company='AnotherCompany', Sector='Retail'),
            ]
        )
        self.assertEqual(
            left.join(right, 'Company').to_records(),
            [
                dict(Name='John', Company='AnyCompany',
                     Age=42, Sector='Finance'),
                dict(Name='Jane', Company='AnyCompany',
                     Age=37, Sector='Finance'),
                dict(Name='Jean', Company='SomeCompany',
                     Age=23, Sector='Consulting'),
            ]
        )
        with self.assertRaises(DataFrameException):
            _ = left.join(right, 'Age')
        with self.assertRaises(DataFrameException):
            _ = left.join(right, 'Country')

    def test_sort(self):
        records = [dict(Name='Jane', Age=43), dict(Name='Jean', Age=21)]
        df = DataFrame.from_records(records)
        self.assertEqual(df.sort('Name').to_records(), records)
        self.assertEqual(df.sort('Age').to_records(), list(reversed(records)))
        with self.assertRaises(DataFrameException):
            _ = df.sort('LastName')

    def test_group(self):
        df = DataFrame.from_records(
            [
                dict(Breed='Australian Shepherd', Age=4, Toy='Bunny'),
                dict(Breed='Australian Shepherd', Age=6, Toy='Sheep'),
                dict(Breed='Beagle', Age=4, Toy='Bunny'),
            ]
        )
        self.assertEqual(
            df.group('Breed').to_records(),
            [
                dict(Breed='Australian Shepherd', Age=[4, 6], Toy=['Bunny', 'Sheep']),
                dict(Breed='Beagle', Age=[4], Toy=['Bunny']),
            ]
        )
        self.assertEqual(
            df.group('Age', 'Toy').to_records(),
            [
                dict(Age=4, Toy='Bunny', Breed=['Australian Shepherd', 'Beagle']),
                dict(Age=6, Toy='Sheep', Breed=['Australian Shepherd']),
            ]
        )
        with self.assertRaises(DataFrameException):
            _ = df.group('TailLength')

    def test_select(self):
        df = DataFrame.from_records([dict(Fruit='Banana', Rating=5, Color='Yellow'), dict(Fruit='Apple', Rating=7, Color='Red')])
        self.assertEqual(
            df.select('Fruit', 'Color').to_records(),
            [dict(Fruit='Banana', Color='Yellow'), dict(Fruit='Apple', Color='Red')]
        )
        with self.assertRaises(DataFrameException):
            _ = df.select('Fruit', 'Size')

    def test_apply(self):
        df = DataFrame.from_records(
            [
                dict(Name='AnyCompany', Revenue=1_000),
                dict(Name='SomeCompany', Revenue=2_000),
            ]
        )
        self.assertEqual(
            df.apply('RevenueThousands', lambda revenue: revenue // 1_000, 'Revenue').to_records(),
            [
                dict(Name='AnyCompany', Revenue=1_000, RevenueThousands=1),
                dict(Name='SomeCompany', Revenue=2_000, RevenueThousands=2),
            ]
        )
        with self.assertRaises(AssertionError):
            _ = df.apply('Revenue', lambda revenue: revenue // 1_000, 'Revenue')
        with self.assertRaises(DataFrameException):
            _ = df.apply('TitledLocation', lambda location: location.title(), 'Location')

    def test_extract(self):
        df = DataFrame.from_records(
            [
                dict(Experiment=1, Result=42),
                dict(Experiment=2, Result=37),
            ]
        )
        self.assertEqual(df.extract('Result'), [42, 37])
        with self.assertRaises(DataFrameException):
            _ = df.extract('Measurement')

    def test_to_string(self):
        self.assertEqual(
            DataFrame.from_records(
                [
                    dict(Temperature=27, Date='2010-05-07'),
                    dict(Temperature=24, Date='2013-08-28'),
                ]
            ).select('Date', 'Temperature').to_string().splitlines(),
            [
                'Date       Temperature',
                '----       -----------',
                '2010-05-07 27         ',
                '2013-08-28 24         ',
            ]
        )

    def test_to_json(self):
        self.assertEqual(
            DataFrame.from_records([dict(Color='Green', Example='Pea')]).to_json().splitlines(),
            [
                '[',
                '    {',
                '        "Color": "Green",',
                '        "Example": "Pea"',
                '    }',
                ']',
            ]
        )

    def test_to_csv(self):
        self.assertEqual(
            DataFrame.from_records([dict(Season='Summer', Example='Beach')]).select('Season', 'Example').to_csv().splitlines(),
            [
                'Season,Example',
                'Summer,Beach',
            ]
        )

    def test_equality(self):
        left = DataFrame.from_records([dict(Language='Italian', Rating=7), dict(Language='English', Rating=6)])
        right = DataFrame.from_records([dict(Language='Italian', Rating=7)])
        self.assertEqual(left.filter('Language', 'Italian'), right)

    def test_getitem(self):
        df = DataFrame.from_records([dict(Language='Italian', Rating=7), dict(Language='English', Rating=6)])
        self.assertEqual(df[:], df)
        self.assertEqual(df[:1], DataFrame.from_records([dict(Language='Italian', Rating=7)]))
        self.assertEqual(df[0], dict(Language='Italian', Rating=7))
