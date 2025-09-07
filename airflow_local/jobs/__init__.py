from pathlib import Path
from dotenv import load_dotenv

cur = Path(__file__).resolve()
for p in [cur] + list(cur.parents):
    env = p / ".env"
    if env.exists():
        load_dotenv(env, override=False)
        break
