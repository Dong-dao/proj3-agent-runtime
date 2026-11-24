"""pytest 共享配置：把仓库根目录加入 sys.path，使 tests 可直接导入 runtime 包。"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
