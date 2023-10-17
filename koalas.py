from itertools import product
import json
from types import FunctionType
from collections import defaultdict


class DataFrame:

    """Class to represent, manipulate, inspect and export tabular data."""

    @staticmethod
    def from_records(records:list[dict[str, object]]) -> 'DataFrame':
        """
        Initializes the DataFrame given a collection of records.

        Parameters
        ----------
            records: list[dict[str, object]]
                The records representing the DataFrame.
            
        Return
        ------
            DataFrame
        """
        records = list(records)
        fields = sorted({key for record in records for key in record})
        rows = [tuple(record.get(field) for field in fields) for record in records]
        return DataFrame(fields=fields, rows=rows)

    def __init__(self, rows, fields):
        self.rows = [tuple(row) for row in rows]
        self.fields = tuple(fields)

    def __getitem__(self, obj):
        if isinstance(obj, int):
            return dict(zip(self.fields, self.rows[obj]))
        return DataFrame(fields=self.fields, rows=self.rows[obj])
    
    def __iter__(self):
        return iter(self.to_records())
    
    def __len__(self):
        return len(self.rows)
    
    def __str__(self):
        return self.to_string()
    
    def __repr__(self):
        return str(self)
    
    def __hash__(self):
        return hash((tuple(self.fields), tuple(self.rows)))
    
    def __eq__(self, other):
        return hash(self) == hash(other)
    
    def reverse(self) -> 'DataFrame':
        """
        Reverses the order of rows.

        Return
        ------
            DataFrame
        """
        return DataFrame(fields=self.fields, rows=reversed(self.rows))
    
    def export(self, format:str, path:str, **kwargs) -> 'DataFrame':
        """
        Exports the DataFrame instance into a file with the specified format (one of 'csv', 'json'
        or 'string').

        Parameters
        ----------
            format: str
                The output format (one of 'csv', 'json' or 'string').
            path: str
                The path where to write the contents to.
            **kwargs
                Optional arguments to be propagated to the function generating the contents.

        Return
        ------
            DataFrame
        """
        f = getattr(self, f'to_{format}', None)
        if not f:
            raise DataFrameException(f'Exporting format "{format}" is not valid.')
        text = f(**kwargs)
        with open(path, 'w') as f:
            f.write(text)
            f.close()
        return self
    
    def to_csv(self) -> str:
        """
        Returns a CSV representation of the DataFrame.

        Return
        ------
            str
        """
        return '\n'.join(','.join(map(str, row)) for row in (self.fields, *self.rows))

    def to_string(self) -> str:
        """
        Returns a textual representation of the DataFrame.

        Return
        ------
            str
        """
        maximum_length = 42 # TODO add configuration file
        shorten = lambda text: text if len(str(text)) <= maximum_length else f'{str(text)[:maximum_length]}...'
        rows = [self.fields] + [['-'*len(field) for field in self.fields]] + self.rows
        rows = [[shorten(value) for value in row] for row in rows]
        widths = [max(len(str(row[i])) for row in rows) for i in range(len(self.fields))]
        lines = [' '.join(str(value).ljust(width) for width, value in zip(widths, row)) for row in rows]
        return '\n'.join(lines)
    
    def to_records(self) -> list[dict[str, object]]:
        """
        Returns the DataFrame as a list of records.

        Return
        ------
            list[dict[str, object]]
        """
        return [dict(zip(self.fields, row))for row in self.rows]
    
    def to_json(self) -> str:
        """
        Returns a JSON representation as a list of records.

        Return
        ------
            str
        """
        return json.dumps(self.to_records(), indent=4)
    
    def rename(self, field:str, new:str) -> 'DataFrame':
        """
        Renames the given field with the given name.

        Parameters
        ----------
            field: str
                The field to be renamed.
            new: str
                The new name of the field.

        Return
        ------
            DataFrame        
        """
        index = self._get_field_index(field)
        return DataFrame(
            fields=self.fields[:index] + (new,) + self.fields[index+1:],
            rows=self.rows
        )
    
    def join(self, other:'DataFrame', *fields:str) -> 'DataFrame':
        """
        Joins with the other DataFrame on the specified fields; in particular, it performs a left,
        inner join.

        Parameters
        ----------
            other: DataFrame
                The DataFrame to join with.
            *fields: str
                The common fields to join on.

        Return
        ------
            DataFrame
        """
        left_fields = [field for field in self.fields if not field in fields]
        right_fields = [field for field in other.fields if not field in fields]
        left, right = self.select(*fields, *left_fields), other.select(*fields, *right_fields)
        common = set(left.fields) & set(right.fields) - set(fields)
        for field in common: left = left.rename(field, f'{field} (1)')
        for field in common: right = right.rename(field, f'{field} (2)')
        grouped = defaultdict(lambda: [[], []])
        for i, df in enumerate((left, right)):
            for row in df.rows:
                key = row[:len(fields)]
                grouped[key][i].append(row[len(fields):])
        return DataFrame(
            fields=[*fields, *left.fields[len(fields):], *right.fields[len(fields):]],
            rows=[
                (*key, *a, *b)
                for key in sorted(grouped)
                for a, b in product(*grouped[key])
            ]
        )        
    
    def sort(self, *fields:str) -> 'DataFrame':
        """
        Returns a DataFrame instance in which rows are sorted by the values for the given fields.

        Parameters
        ----------
            *fields: str
                The fields to sort by.

        Return
        ------
            DataFrame
        """
        indices = [self._get_field_index(f) for f in fields]
        return DataFrame(
            fields=self.fields,
            rows=sorted(self.rows, key=lambda row: tuple(row[i] for i in indices))
        )
    
    def extract(self, field:str) -> list[object]:
        """
        Returns the values for the given field.

        Parameters
        ----------
            field: str

        Return
        ------
            list[object]
        """
        index = self._get_field_index(field)
        return [row[index] for row in self.rows]
    
    def select(self, *fields:str) -> 'DataFrame':
        """
        Returns a DataFrame instance in which only the given fields are retained.

        Parameters
        ----------
            *fields: str
                The fields to retain.

        Return
        ------
            DataFrame        
        """
        indices = [self._get_field_index(field) for field in fields]
        return DataFrame(
            fields=fields,
            rows=[tuple(row[i] for i in indices) for row in self.rows]
        )
    
    def apply(self, field:str, fn:FunctionType, *fields:str) -> 'DataFrame':
        """
        Returns a DataFrame instance in which the given field is the result of the application of
        the given function for the values in the given fields.

        Parameters
        ----------
            field: str
                The name of the field to generate for the result of the application.
            fn: function[*object] -> object
                The function to apply.            
            *fields: str
                The names of the field the values of which will have the function applied to.

        Return
        ------
            DataFrame

        Example
        -------
            >>> df = (
            ...     DataFrame
            ...         .from_records([dict(A=1, B=2), dict(A=3, B=4)])
            ...         .apply('Average', lambda a, b: a + b / 2, 'A', 'B')
            ... )
            ...
            >>> df
            A B Average
            - - -------
            1 2 1.5
            3 4 3.5
        """

        assert field not in self.fields
        indices = [self._get_field_index(f) for f in fields]
        rows = [(*row, fn(*(row[i] for i in indices))) for row in self.rows]

        return DataFrame(fields=self.fields+(field,), rows=rows)
    
    def group(self, *fields:str) -> 'DataFrame':
        """
        Returns a DataFrame instance in which rows are grouped by values for the given fields.

        Parameters
        ----------
            *fields: str
                The name of the fields to group rows by.

        Return
        ------
            DataFrame

        Example
        -------
            >>> df = (
            ...     DataFrame
            ...         .from_records([dict(A=1, B=2, C=3), dict(A=1, B=2, C=4), dict(A=0, B=0, C=0)])
            ...         .group('A', 'B')
            ... )
            ...
            >>> df
            A B C     
            - - -     
            0 0 [0]   
            1 2 [3, 4]
        """
        key_indices = [self._get_field_index(f) for f in fields]
        other_indices = [i for i in range(len(self.fields)) if i not in key_indices]
        grouped = {}
        for row in self.rows:
            key  = tuple(row[i] for i in key_indices)
            if key not in grouped:
                grouped[key] = [[] for _ in range(len(self.fields))]
            for i, value in enumerate(row):
                grouped[key][i].append(value)
        return DataFrame(
            fields=tuple(fields) + tuple(f for f in self.fields if f not in fields),
            rows=[
                (*key,) + tuple(row[i] for i in other_indices)
                for key, row in sorted(grouped.items())
            ]
        )

    def filter(self, field:str, value:object) -> 'DataFrame':
        """
        Returns a DataFrame instance in which rows are filtered on the given field for the given
        value.

        Parameters
        ----------
            field: str
                The name of the field to filter by.
            value: object
                The value that the row must have for the given field to be retained.

        Return
        ------
            DataFrame
        """
        index = self._get_field_index(field)
        return DataFrame(
            fields=self.fields,
            rows=[row for row in self.rows if row[index] == value],            
        )
    
    def _get_field_index(self, field):
        if field not in self.fields:
            raise DataFrameException(f'Field "{field}" not found.')
        return self.fields.index(field)
    

class DataFrameException(Exception):

    pass