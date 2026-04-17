from peewee import PostgresqlDatabase, Model, AutoField, IntegerField, CharField, DateTimeField, TextField, \
                ForeignKeyField, DecimalField, BigIntegerField, UUIDField
from settings import DB_STAGING_HOST, DB_STAGING_PORT, DB_STAGING_NAME, DB_STAGING_USER, DB_STAGING_PASSWORD
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
    pass

class FlatModels(LowCardinalDimension):
    pass

# Normalized
class HDBListings(BaseModel):
    month = DateTimeField(formats='%Y-%m')
    town_id = ForeignKeyField(Towns, backref='listings')
    flat_type_id = ForeignKeyField(FlatTypes, backref='listings')
    block = CharField(200),
    street_id = ForeignKeyField(Streets, backref='listings')
    storey_range_from = IntegerField()
    storey_range_to = IntegerField()
    floor_area_sqm = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    flat_model_id = ForeignKeyField(FlatModels, backref='listings')
    lease_commence_date = IntegerField()    # Year
    remaining_lease_year = IntegerField()
    remaining_lease_month = IntegerField()
    resale_price = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    _id = IntegerField()


class HDBListingsRaw(BaseModel):
    ingestion_id = UUIDField()
    month = CharField(max_length=7)
    town = CharField(max_length=200)
    flat_type = CharField(max_length=200)
    block = CharField(200)
    street_name = CharField(max_length=200)
    storey_range_from = IntegerField()
    storey_range_to = IntegerField()
    floor_area_sqm = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    flat_model = CharField(max_length=200)
    lease_commence_date = IntegerField()    # Year
    remaining_lease_year = IntegerField()
    remaining_lease_month = IntegerField()
    resale_price = DecimalField(max_digits=10, decimal_places=2, auto_round=True)
    


db_staging.create_tables([HDBListingsRaw, 
                          HDBListings,
                          Towns,
                          FlatTypes,
                          Streets,
                          FlatModels])