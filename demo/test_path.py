# import numpy as np
# for i in np.arange(70, 140, 0.5):
#     print(i)
from datetime import timedelta, datetime

now = datetime.today()
rounded_hour = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
days_30 = [str(rounded_hour + timedelta(days=i)) for i in range(30)]
print(days_30)
