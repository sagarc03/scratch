from enum import IntEnum, auto

from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    Executable,
    Integer,
    MetaData,
    Row,
    Table,
    and_,
    create_engine,
    func,
    or_,
    select,
)


class DBType(IntEnum):
    SQLite = auto()


def get_db(db: DBType, url: str | None = None) -> Engine:
    if db == DBType.SQLite and url is None:
        url = "sqlite://"

    if url is None:
        raise NotImplementedError("Given db is not implemented.")

    return create_engine(url)


def print_table(data: list[Row]) -> None:
    columns = data[0]._fields
    max_col_length = max((len(c) for c in columns)) + 3
    label = f"{{:^{max_col_length}}}" * len(columns)
    print(label.format(*columns))
    print("-" * (len(columns) * max_col_length))
    for each in data:
        print(label.format(*[x if x is not None else "NULL" for x in each]))


metadata_obj = MetaData()

Events = Table(
    "EVENTS",
    metadata_obj,
    Column("ID", Integer, primary_key=True),
    Column("CATEGORY_ID", Integer, nullable=False),
    Column("SET_ID", Integer, nullable=False),
    Column("STATUS", Integer, nullable=False, default=0),
    Column("CREATED_AT", DateTime(timezone=True), server_default=func.now()),
)


next_category_job = (
    select(Events.c.CATEGORY_ID, func.min(Events.c.ID).label("NEXT_CATEGORY_JOB_ID"))
    .where(and_(Events.c.STATUS.in_([0, 1]), Events.c.SET_ID == -1))
    .group_by(Events.c.CATEGORY_ID)
    .subquery("next_category_job")
)

number_of_jobs = (
    select(Events.c.CATEGORY_ID, func.count().label("NUMBER_OF_RUNNING_JOBS"))
    .where(Events.c.STATUS == 1)
    .group_by(Events.c.CATEGORY_ID)
    .subquery("number_of_jobs")
)

data: list[tuple[int, int]] = [
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 1),
    (2, 2),
    (1, -1),
    (1, 4),
    (1, 7),
    (2, 3),
    (2, 4),
    (2, -1),
    (2, 5),
    (1, 5),
    (1, 6),
    (2, 6),
    (2, 7),
]


def create_tables():
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    Events.create(engine)


def insert_data():
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    with engine.begin() as connention:
        connention.execute(
            Events.insert(),
            [
                {"CATEGORY_ID": category_id, "SET_ID": set_id}
                for category_id, set_id in data
            ],
        )


def get_all_jobs():
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    with engine.begin() as connention:
        result = connention.execute(
            select(Events.c.ID, Events.c.CATEGORY_ID, Events.c.SET_ID, Events.c.STATUS)
        ).all()
    print_table(result)


def ready_jobs_query() -> Executable:
    return (
        select(
            Events.c.ID,
            Events.c.CATEGORY_ID,
            Events.c.SET_ID,
            Events.c.STATUS,
            next_category_job.c.NEXT_CATEGORY_JOB_ID.label("NEXT_INDEP_JOB_ID"),
            number_of_jobs.c.NUMBER_OF_RUNNING_JOBS.label("NO_OF_CATEGORY_JOBS"),
        )
        .join(
            next_category_job,
            Events.c.CATEGORY_ID == next_category_job.c.CATEGORY_ID,
            isouter=True,
        )
        .join(
            number_of_jobs,
            Events.c.CATEGORY_ID == number_of_jobs.c.CATEGORY_ID,
            isouter=True,
        )
    )


def get_ready_jobs_before_filters():
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    query = ready_jobs_query()
    with engine.begin() as connention:
        result = connention.execute(query).all()
    print_table(result)


def get_ready_jobs():
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    query = ready_jobs_query().where(
        and_(
            Events.c.STATUS == 0,
            or_(
                and_(
                    Events.c.SET_ID != -1,
                    or_(
                        next_category_job.c.NEXT_CATEGORY_JOB_ID.is_(None),
                        next_category_job.c.NEXT_CATEGORY_JOB_ID >= Events.c.ID,
                    ),
                ),
                and_(
                    Events.c.SET_ID == -1,
                    or_(
                        number_of_jobs.c.NUMBER_OF_RUNNING_JOBS.is_(None),
                        number_of_jobs.c.NUMBER_OF_RUNNING_JOBS == 0,
                    ),
                ),
            ),
        )
    )
    with engine.begin() as connention:
        result = connention.execute(query).all()

    print_table(result)


def get_job():
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    side_events = (
        Events.select()
        .where(and_(Events.c.SET_ID == -1, Events.c.STATUS == 1))
        .subquery("SIDE_EVENTS")
    )
    number_of_jobs = (
        select(Events.c.CATEGORY_ID, func.count().label("NUMBER_OF_RUNNING_JOBS"))
        .where(Events.c.STATUS == 1)
        .group_by(Events.c.CATEGORY_ID)
        .subquery("number_of_jobs")
    )
    query = (
        select(
            Events.c.ID,
            Events.c.CATEGORY_ID,
            Events.c.SET_ID,
            Events.c.STATUS,
            side_events.c.ID.label("CROSS_CATEGORY_ID"),
            number_of_jobs.c.NUMBER_OF_RUNNING_JOBS.label("NUMBER_OF_RUNNING_JOBS"),
        )
        .join(
            side_events, Events.c.CATEGORY_ID == side_events.c.CATEGORY_ID, isouter=True
        )
        .join(
            number_of_jobs,
            Events.c.CATEGORY_ID == number_of_jobs.c.CATEGORY_ID,
            isouter=True,
        )
        .where(and_(Events.c.STATUS == 0, side_events.c.ID.is_(None)))
        .with_for_update()
    )

    with engine.begin() as connention:
        while True:
            result = connention.execute(query).first()
            if not result:
                return
            if (
                result.SET_ID == -1
                and result.NUMBER_OF_RUNNING_JOBS
                and result.NUMBER_OF_RUNNING_JOBS > 0
            ):
                query = query.where(Events.c.CATEGORY_ID != result.CATEGORY_ID)
                continue
            break
        connention.execute(
            Events.update().where(Events.c.ID == result.ID).values({"STATUS": 1})
        )
    print_table(result)


def get_job_by_id(job_id: int):
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    with engine.begin() as connention:
        connention.execute(
            Events.update().where(Events.c.ID == job_id).values({"STATUS": 1})
        )


def complete_job(job_id: int):
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    with engine.begin() as connention:
        connention.execute(
            Events.update().where(Events.c.ID == job_id).values({"STATUS": 2})
        )


def reset_job(job_id: int):
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    with engine.begin() as connention:
        connention.execute(
            Events.update().where(Events.c.ID == job_id).values({"STATUS": 0})
        )


def reset_all_jobs():
    engine = get_db(DBType.SQLite, "sqlite:///data.db")
    with engine.begin() as connention:
        connention.execute(Events.update().values({"STATUS": 0}))
