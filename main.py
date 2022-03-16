import base64
import migration
from loguru import logger
from sqlalchemy import create_engine, MetaData, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_database, database_exists

def __create_engine(url):
    try:
        __url = base64.b64decode(url.encode('utf-8')).decode('utf-8')
    except Exception as e:
        __url = url
    try:
        if __url.startswith('sqlite'):
            return create_engine(__url, connect_args={'timeout': 10, 'check_same_thread': False})

        elif __url.startswith('mysql'):
            __engine = create_engine(__url, pool_recycle=500, pool_size=50, max_overflow=60, pool_timeout=600)
            if not database_exists(__engine.url):
                create_database(__engine.url)
            return __engine

        return create_engine(__url, encoding='utf-8', pool_recycle=500, pool_size=50, max_overflow=60, pool_timeout=600)
    except Exception as e:
        logger.exception('Engine create error. {}'.format(e))
        return None



convention = {
    "ix": "IX_%(column_0_label)s",
    "uq": "UQ_%(table_name)s_%(column_0_name)s",
    "ck": "CK_%(table_name)s_%(constraint_name)s",
    "fk": "FK_%(table_name)s_%(referred_table_name)s",
    "pk": "PK_%(table_name)s"
}
metadata = MetaData(naming_convention=convention)

class SQLAlchemyDBConnection:
    """SQLAlchemy database connection"""
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.engine = create_engine(self.connection_string, connect_args={'timeout': 30, 'check_same_thread': False})
        self.Session = sessionmaker(autocommit=False, autoflush=True, bind=self.engine)

    # with구문 진입시에 db와 connection을 하고
    # ORM을 사용하기 위한 session을 만들어준다.
    def __enter__(self):
        self.session = self.Session()
        return self

    # with구문을 빠져나오기 전 session의 종료를 한다.
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

TableBase = declarative_base(cls=YOUR_DB_SCHEMA_CLASS, metadata=metadata)

def create():
    db_context = SQLAlchemyDBConnection('YOUR_DB_URL')
    engine = db_context.engine
    logger.info("engine_name={}".format(engine.name))

    with engine.begin() as conn:
        if engine.name == 'mysql':
            conn.execute('SET FOREIGN_KEY_CHECKS=0;')
        if engine.name == 'sqlite':
            conn.execute('PRAGMA foreign_keys=OFF;')

        migration.check_db(conn)

        if engine.name == 'mysql':
            conn.execute('SET FOREIGN_KEY_CHECKS=1;')
        if engine.name == 'sqlite':
            conn.execute('PRAGMA foreign_keys=ON;')

    TableBase.metadata.create_all(engine)
