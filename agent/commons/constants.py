import os


class Constants:
    INTERNAL_MESSAGE_TAGGING_BASE = "devo.collectors.out"
    COLLECTOR_IMAGE = os.environ.get('COLLECTOR_IMAGE')
    BANNER_1 = """
  _____                              _ _           _             
 |  __ \                            | | |         | |            
 | |  | | _____   _____     ___ ___ | | | ___  ___| |_ ___  _ __ 
 | |  | |/ _ \ \ / / _ \   / __/ _ \| | |/ _ \/ __| __/ _ \| '__|
 | |__| |  __/\ V / (_) | | (_| (_) | | |  __/ (__| || (_) | |   
 |_____/ \___| \_/ \___/   \___\___/|_|_|\___|\___|\__\___/|_|   
"""
    BANNER_2 = """
    ____                                ____          __            
   / __ \___ _   ______     _________  / / /__  _____/ /_____  _____
  / / / / _ \ | / / __ \   / ___/ __ \/ / / _ \/ ___/ __/ __ \/ ___/
 / /_/ /  __/ |/ / /_/ /  / /__/ /_/ / / /  __/ /__/ /_/ /_/ / /    
/_____/\___/|___/\____/   \___/\____/_/_/\___/\___/\__/\____/_/                                                                         
"""
    BANNER_3 = """
  ____                              _ _           _             
 |  _ \  _____   _____     ___ ___ | | | ___  ___| |_ ___  _ __ 
 | | | |/ _ \ \ / / _ \   / __/ _ \| | |/ _ \/ __| __/ _ \| '__|
 | |_| |  __/\ V / (_) | | (_| (_) | | |  __/ (__| || (_) | |   
 |____/ \___| \_/ \___/   \___\___/|_|_|\___|\___|\__\___/|_|                                                          
"""

    BANNER_4 = """\033[32m
██████╗ ███████╗██╗   ██╗ ██████╗      ██████╗ ██████╗ ██╗     ██╗     ███████╗ ██████╗████████╗ ██████╗ ██████╗ 
██╔══██╗██╔════╝██║   ██║██╔═══██╗    ██╔════╝██╔═══██╗██║     ██║     ██╔════╝██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗
██║  ██║█████╗  ██║   ██║██║   ██║    ██║     ██║   ██║██║     ██║     █████╗  ██║        ██║   ██║   ██║██████╔╝
██║  ██║██╔══╝  ╚██╗ ██╔╝██║   ██║    ██║     ██║   ██║██║     ██║     ██╔══╝  ██║        ██║   ██║   ██║██╔══██╗
██████╔╝███████╗ ╚████╔╝ ╚██████╔╝    ╚██████╗╚██████╔╝███████╗███████╗███████╗╚██████╗   ██║   ╚██████╔╝██║  ██║
╚═════╝ ╚══════╝  ╚═══╝   ╚═════╝      ╚═════╝ ╚═════╝ ╚══════╝╚══════╝╚══════╝ ╚═════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝                                                                                               
\033[0m"""
    BANNER_5 = """
 __   ___       __      __   __             ___  __  ___  __   __  
|  \ |__  \  / /  \    /  ` /  \ |    |    |__  /  `  |  /  \ |__) 
|__/ |___  \/  \__/    \__, \__/ |___ |___ |___ \__,  |  \__/ |  \ 
"""
