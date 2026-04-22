from peewee import PostgresqlDatabase, Model, AutoField, IntegerField, CharField, DateTimeField, TextField, \
                ForeignKeyField, DecimalField, BigIntegerField, UUIDField, DateField, EXCLUDED
from settings import DB_STAGING_HOST, DB_STAGING_PORT, DB_STAGING_NAME, DB_STAGING_USER, DB_STAGING_PASSWORD
from typing import Self
from datetime import datetime, timedelta
import json

db_staging = PostgresqlDatabase(database=DB_STAGING_NAME, host=DB_STAGING_HOST, port=DB_STAGING_PORT, user=DB_STAGING_USER, password=DB_STAGING_PASSWORD)


# Peewee doesn't have JSONField by default. Create one as a subclass of TextField + some validation
class JsonField(TextField):
    field_type = 'JSON'
    
    def adapt(self, value):
        if type(value) == dict:
            val = json.dumps(value)
            return val
        else:
            val = super().adapt(value)
            try:
                json.loads(val)
                return val
            except Exception as err:
                raise ValueError(f'Value is not JSON formattable: {value}')
    
    def python_value(self, value):
        if not value:
            return None
        val = json.loads(super().python_value(value))
        return val


class BaseModel(Model):
    class Meta:
        database = db_staging
        legacy_table_names = False
    
    id = AutoField()
    created_at = DateTimeField(default=datetime.now)


class LowCardinalDimension(BaseModel):
    name = CharField(200, unique=True)

    @classmethod
    def get_id_dict(cls):
        result = cls.select(cls.id, cls.name)
        mapp = {}
        for i in result:
            mapp[i.name] = i.id
        return mapp

class Towns(LowCardinalDimension):
    pass

class FlatTypes(LowCardinalDimension):
    pass

class Streets(LowCardinalDimension):
    # Assuming Singapore has streets with the same names but from different towns
    class Meta:
        # Unique name-town combination
        indexes = [(('name', 'town'), True)]

    name = CharField(200)
    town = ForeignKeyField(Towns, backref='streets')
    
    @classmethod
    def get_id_dict(cls):
        result = cls.select(cls.id, cls.name, cls.town)
        mapp = {}
        for i in result:
            key = str(i.town) + '__' + i.name
            mapp[key] = i.id
        return mapp

class FlatModels(LowCardinalDimension):
    pass


# Raw
class HDBListingsRaw(BaseModel):
    ingestion_id = UUIDField()
    batch_id = UUIDField()
    _id = IntegerField()
    month = CharField(max_length=7)
    town = CharField(max_length=200)
    flat_type = CharField(max_length=200)
    block = CharField(max_length=200)
    street_name = CharField(max_length=200)
    storey_range = CharField(max_length=12)
    floor_area_sqm = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    flat_model = CharField(max_length=200)
    lease_commence_date = IntegerField()    # Year
    remaining_lease = CharField(max_length=50)
    resale_price = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    

# Normalized
class HDBListingsNormalized(BaseModel):
    ingestion_id = UUIDField()
    _id = IntegerField()
    month = CharField(7)
    town = ForeignKeyField(Towns, backref='listings')
    flat_type = ForeignKeyField(FlatTypes, backref='listings')
    block = CharField(200)
    street = ForeignKeyField(Streets, backref='listings')
    storey_range_from = IntegerField()
    storey_range_to = IntegerField()
    floor_area_sqm = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    flat_model = ForeignKeyField(FlatModels, backref='listings')
    lease_commence_date = IntegerField()    # Year
    remaining_lease_year = IntegerField()
    remaining_lease_month = IntegerField()
    resale_price = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    full_address = CharField(max_length=255, unique=True )       # Identifier
    _update_columns = {}

    @classmethod
    def upsert_many(cls, model_list: list[Self]):
        if not cls._update_columns:
            cols = cls._meta.columns
            for c, v in cols.items():
                cls._update_columns[c] = EXCLUDED.__getattr__(c)

        dict_list = [i.__dict__['__data__'] for i in model_list]
        return cls.insert_many(dict_list).on_conflict(conflict_target=(cls.full_address,), update=cls._update_columns).execute()
        # return cls.insert_many(model_list).on_conflict_replace()


db_staging.drop_tables([
    Towns,
    FlatTypes,
    Streets,
    FlatModels,
    HDBListingsRaw, 
    HDBListingsNormalized,
])

db_staging.create_tables([
    Towns,
    FlatTypes,
    Streets,
    FlatModels,
    HDBListingsRaw, 
    HDBListingsNormalized,
])