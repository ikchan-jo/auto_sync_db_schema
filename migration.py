import sqlalchemy
from alembic.autogenerate import compare_metadata
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.util.sqla_compat import _fk_spec
from loguru import logger

from mmx.db.base import TableBase
from mmx.db.session import engine


def execute_alembic_command(conn, command, op):
    # Commands like add_table, remove_table, add_index, add_column, etc is a
    # tuple and can be handle after running special functions from alembic for
    # them.
    if isinstance(command, tuple):
        # Here methods add_table, drop_index, etc is running. Name of method is
        # the first element of the tuple, arguments to this method comes from
        # the next element(s).
        if command[0] in METHODS:
            logger.info(command)
            METHODS[command[0]](op, *command[1:])
        else:
            pass
            # LOG.warning(_LW("Ignoring alembic command %s"), command[0])
    elif isinstance(command, list):
        # Here methods modify_type is running.
        if command[0][0] in METHODS:
            logger.info(command)
            METHODS[command[0][0]](conn, op, *command[0][1:])
        else:
            pass
            # LOG.warning(_LW("Ignoring alembic command %s"), command[0])


METHODS = {}


def column_names(obj):
    return [col.name for col in obj.columns if hasattr(col, 'name')]


def alembic_command_method(f):
    METHODS[f.__name__] = f
    return f


@alembic_command_method
def add_table(op, table):  # test ok
    # Check if table has already exists and needs just to be renamed
    table.create(bind=op.get_bind(), checkfirst=True)


@alembic_command_method
def remove_table(op, table):  # test ok
    op.drop_table(table)


@alembic_command_method
def add_index(op, index):  # test ok
    bind = op.get_bind()
    insp = sqlalchemy.engine.reflection.Inspector.from_engine(bind)
    if index.name not in [idx['name'] for idx in
                          insp.get_indexes(index.table.name)]:

        if engine.name == 'sqlite':
            with op.batch_alter_table(index.table.name, recreate='always') as batch_op:
                try:
                    batch_op.create_index(index.name, column_names(index))
                except sqlalchemy.exc.OperationalError as e:
                    if 'already exists' in e:
                        batch_op.create_index('{}_{}'.format(index.name, 1), column_names(index))
                    else:
                        raise e

        else:
            op.create_index(index.name, index.table.name, column_names(index))


@alembic_command_method
def remove_index(op, index):
    bind = op.get_bind()
    insp = sqlalchemy.engine.reflection.Inspector.from_engine(bind)
    index_names = [idx['name'] for idx in insp.get_indexes(index.table.name)]
    fk_names = [i['name'] for i in insp.get_foreign_keys(index.table.name)]
    if index.name in index_names and index.name not in fk_names:
        if engine.name == 'sqlite':
            with op.batch_alter_table(index.table.name, recreate='always') as batch_op:
                batch_op.drop_index(index.name)
        else:
            op.drop_index(index.name, index.table.name)


@alembic_command_method  # test ok
def add_column(op, schema, table_name, column):
    if engine.name == 'sqlite':
        with op.batch_alter_table(table_name) as batch_op:
            batch_op.add_column(column.copy())
    else:
        op.add_column(table_name, column.copy(), schema=schema)


@alembic_command_method  # test ok
def remove_column(op, schema, table_name, column):
    if engine.name == 'sqlite':
        with op.batch_alter_table(table_name, recreate='always') as batch_op:
            batch_op.drop_column(column.name)
    else:
        op.drop_column(table_name, column.name, schema=schema)


@alembic_command_method
def add_constraint(op, constraint):
    if engine.name == 'sqlite':
        with op.batch_alter_table(constraint.table.name) as batch_op:
            batch_op.create_unique_constraint(constraint.name,
                                              column_names(constraint))
    else:
        op.create_unique_constraint(constraint.name, constraint.table.name,
                                    column_names(constraint))


@alembic_command_method
def remove_constraint(op, constraint):
    if engine.name == 'sqlite':
        with op.batch_alter_table(constraint.table.name) as batch_op:
            op.drop_constraint(constraint.name, type_='unique')
    else:
        op.drop_constraint(constraint.name, constraint.table.name, type_='unique')


@alembic_command_method
def add_fk(op, fk):  # test ok
    fk_name = fk.name
    # As per Mike Bayer's comment, using _fk_spec method is preferable to
    # direct access to ForeignKeyConstraint attributes
    fk_spec = _fk_spec(fk)
    fk_table = fk_spec[1]
    fk_ref = fk_spec[4]
    fk_local_cols = fk_spec[2]
    fk_remote_cols = fk_spec[5]
    fk_onupdate = fk_spec[6]
    fk_ondelete = fk_spec[7]

    if not fk_name:
        fk_name = "fk_{}_{}".format(fk_ref.lower(), fk_remote_cols[0])

    if engine.name == 'sqlite':
        with op.batch_alter_table(fk_table, recreate='always') as batch_op:
            batch_op.create_foreign_key(op.f(fk_name), fk_ref, fk_local_cols,
                                        fk_remote_cols, onupdate=fk_onupdate, ondelete=fk_ondelete)
    else:
        op.create_foreign_key(op.f(fk_name), fk_table, fk_ref, fk_local_cols,
                              fk_remote_cols, onupdate=fk_onupdate, ondelete=fk_ondelete)


@alembic_command_method
def remove_fk(op, fk):  # test ok
    if engine.name == 'sqlite':
        with op.batch_alter_table(fk.parent.name, recreate='always') as batch_op:
            batch_op.drop_constraint(fk.name, type_='foreignkey')
    else:
        op.drop_constraint(fk.name, fk.parent.name, type_='foreignkey')


@alembic_command_method
def modify_type(conn, op, *commands):
    mod_table = commands[1]
    mod_column = commands[2]
    existing_nullable = commands[3]['existing_nullable']
    existing_type = commands[4]
    type_ = commands[5]
    if engine.name == 'sqlite':  # Simulator only
        conn.execute("INSERT INTO TB_MMX_SYSTEM_CONFIG(`key`, value, value_type) VALUES('DB_CLEAN', 'TRUE', 'SYSTEM')")
    else:
        op.alter_column(mod_table, mod_column, existing_type=existing_type,
                        type_=type_, existing_nullable=existing_nullable)


def check_db(conn):
    opts = {
        'compare_type': True,
    }
    mc = MigrationContext.configure(conn, opts=opts)
    op = Operations(mc)
    diff = compare_metadata(mc, TableBase.metadata)
    if diff:
        for el in diff:
            execute_alembic_command(conn, el, op)
