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
        df['time'] = df['time'] - 14 * 60 * 60
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
    
    metadata = MetaData()
    metadata.reflect(engine)
    
    tables = conn.execute('SHOW TABLES').fetchall()
    tables = [table_name for table_name, in tables]
    
    hst_wild_card = os.path.join(hst_data_directory, "*.hst")
    hst_files = glob.glob(hst_wild_card)
    for hst in hst_files:
        name = re.sub("\..+$", "", os.path.basename(hst))
        df = tickdata(hst)
        if name not in tables:
            df.index += 1 
            df.to_sql(name, conn, index_label='id')
            conn.execute('ALTER TABLE `%s` ADD PRIMARY KEY (`id`);'%(name))
            conn.execute('ALTER TABLE `%s` CHANGE `id` `id` INT(11) NOT NULL AUTO_INCREMENT;'%(name))
        else:
            Base.classes
            cl = Base.classes[name]
            latest_date = session.query(cl.time).order_by(cl.id.desc()).first()[0]
            new_df = df[df['time'] > latest_date]
            new_df.to_sql(name, conn, index=False, if_exists='append')

if __name__ == "__main__":
    fire.Fire(record_hst_data_to_mysql)
