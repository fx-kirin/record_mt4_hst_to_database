#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Distributed under terms of the MIT license.

"""

"""
import os
import fire
import glob
import numpy as np
import pandas as pd
import sqlalchemy
import yaml
import re
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import func 
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker

def tickdata(filepath):
    with open(filepath, 'rb') as f:
        ver = np.frombuffer(f.read(148)[:4], 'i4')
        if ver == 400:
            dtype = [('time', 'u4'), ('open', 'f8'), ('low', 'f8'),
                     ('high', 'f8'), ('close', 'f8'), ('volume', 'f8')]
            df = pd.DataFrame(np.frombuffer(f.read(), dtype=dtype))
            df = df['time open high low close volume'.split()]
        elif ver == 401:
            dtype = [('time', 'u8'), ('open', 'f8'), ('high', 'f8'), ('low', 'f8'),
                     ('close', 'f8'), ('volume', 'i8'), ('s', 'i4'), ('r', 'i8')]
            df = pd.DataFrame(np.frombuffer(f.read(), dtype=dtype).astype(dtype[:-2]))
        df['time'] = df['time']
        df['time'] = pd.to_datetime(df['time'], unit='s')
    return df

def create_engine(db_settings):
    return engine

def record_hst_data_to_mysql(hst_data_directory, database_yaml_path):
    with open(database_yaml_path) as f:
        db_settings = yaml.safe_load(f.read())    
        
    engine = sqlalchemy.create_engine("%s://%s:%s@%s:%s"%(db_settings["adapter"], 
        db_settings["username"], db_settings["password"], db_settings["host"], db_settings["port"]), pool_recycle=1800)
    conn = engine.connect()
    try:
        conn.execute("create database %s"%(db_settings["database"]))
    except sqlalchemy.exc.ProgrammingError:
        pass
    conn.close()
        
    engine = sqlalchemy.create_engine("%s://%s:%s@%s:%s/%s"%(db_settings["adapter"], 
        db_settings["username"], db_settings["password"], db_settings["host"], db_settings["port"], db_settings["database"]), pool_recycle=1800)
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    conn = engine.connect()
    
    tables = conn.execute('SHOW TABLES').fetchall()
    tables = [table_name for table_name, in tables]
    
    alchemy_tables = Base.classes.keys()
    for table in tables:
        if table not in alchemy_tables:
            conn.execute('DROP TABLE %s;'%(table))
            
    
    hst_wild_card = os.path.join(hst_data_directory, "*.hst")
    hst_files = glob.glob(hst_wild_card)
    for hst in hst_files:
        name = re.sub("\..+$", "", os.path.basename(hst)).lower()
        df = tickdata(hst)
        df = df[:-1]
        if name not in alchemy_tables:
            df.index += 1 
            create_statement = """
                CREATE TABLE `%s` (
                  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
                  `time` datetime DEFAULT NULL,
                  `open` double DEFAULT NULL,
                  `high` double DEFAULT NULL,
                  `low` double DEFAULT NULL,
                  `close` double DEFAULT NULL,
                  `volume` double DEFAULT NULL,
                  PRIMARY KEY (`id`),
                  KEY `ix_bfnbffh1_id` (`id`)
                ) ENGINE=InnoDB AUTO_INCREMENT=43192 DEFAULT CHARSET=utf8;
            """%(name)
            conn.execute(create_statement)
            df.to_sql(name, conn, index_label='id', if_exists='append', chunksize=1000)
        else:
            Base.classes
            cl = Base.classes[name]
            latest_date = session.query(cl.time).order_by(cl.id.desc()).first()
            if latest_date:
                latest_date = latest_date[0]
                df = df[df['time'] > latest_date]
            df.to_sql(name, conn, index=False, if_exists='append', chunksize=1000)
        del df

if __name__ == "__main__":
    from tendo import singleton
    me = singleton.SingleInstance()
    fire.Fire(record_hst_data_to_mysql)
