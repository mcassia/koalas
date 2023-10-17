from itertools import product
import json
from types import FunctionType


class DataFrame:

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
        rows = [self.fields] + [['-'*len(field) for field in self.fields]] + self.rows
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
    
    def join(self, other:'DataFrame', field:str) -> 'DataFrame':
        """
        Returns a DataFrame instance which is the result of the join of the current instance and
        the given one on the given field. In particular, it performs a left, inner join and it
        requires that the given field is present in both instances and that no other field is
        shared between instances; if either condition is not met, an exception is raised.

        Paramaters
        ----------
            other: DataFrame
                The other DataFrame to join the current with.
            field: str
                The shared field to join on.

        Return
        ------
            DataFrame
        """
        if field not in self.fields or field not in other.fields:
            raise DataFrameException("Joining on field cannot be performed because the field is not present in both tables.")
        left_fields = sorted(set(self.fields) - {field})
        right_fields = sorted(set(other.fields) - {field})
        if set(left_fields) & set(right_fields):
            raise DataFrameException('Joining is not possible because there are common fields.')
        left, right = self.select(field, *left_fields), other.select(field, *right_fields)
        left_grouped, right_grouped = {}, {}
        for df, mapping in [(left, left_grouped), (right, right_grouped)]:
            for row in df.rows:
                if row[0] not in mapping:
                    mapping[row[0]] = []
                mapping[row[0]].append(row[1:])
        return DataFrame(
            fields=[field] + left_fields + right_fields,
            rows=[
                (key, *a, *b)
                for key in sorted(set(left_grouped) & set(right_grouped))
                for a, b in product(left_grouped[key], right_grouped[key])
            ]
        )        
    
    def sort(self, *fields:str, ascending:bool=True) -> 'DataFrame':
        """
        Returns a DataFrame instance in which rows are sorted by the values for the given fields.

        Parameters
        ----------
            *fields: str
                The fields to sort by.
            ascending: bool (default: True)
                Whether to sort in ascending or descending order.

        Return
        ------
            DataFrame
        """
        indices = [self._get_field_index(f) for f in fields]
        return DataFrame(
            fields=self.fields,
            rows=sorted(self.rows, reverse=not ascending, key=lambda row: tuple(row[i] for i in indices))
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