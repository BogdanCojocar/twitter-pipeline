import luigi
from update_operational_db import UpdateOperationalDb

if __name__ == '__main__':
    luigi.run(main_task_cls=UpdateOperationalDb, local_scheduler=True)
