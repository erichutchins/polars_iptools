from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # TypeAlias is in typing from 3.10, which is our minimum.
    from typing import TypeAlias

    import polars as pl
    from polars.datatypes import DataType, DataTypeClass

    IntoExpr: TypeAlias = pl.Expr | str | pl.Series
    PolarsDataType: TypeAlias = DataType | DataTypeClass
