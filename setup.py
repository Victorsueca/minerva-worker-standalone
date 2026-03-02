from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need
# fine tuning.
build_options = {
    'packages': ['rich._unicode_data'],
    'excludes': [],
    'include_files': ['thirdparty'],
    'optimize': 2,
    'include_msvcr': True
}

msi_options = {
    'data': {
        'Directory': [
            ('ProgramMenuFolder', 'TARGETDIR', '.'),
            ('MinervaArchiveMenu', 'ProgramMenuFolder', 'MINERV~1|Minerva Archive')
        ],
        'ProgId': [
            ('Prog.Id', None, None, 'Minerva DPN Worker', 'icon', None)
        ],
        'Property': [
            ('MANUFACTURER', 'The Minerva Archive Contributors'),
        ],
        'Icon': [
            ('icon', 'assets/minerva.ico')
        ]
    },
    'install_icon': 'assets/minerva.ico',
    'license_file': 'assets/license.rtf',
    'summary_data': {
        'author': 'The Minerva Archive Contributors',
        'comments': 'Minerva Worker Standalone'
    },
    'upgrade_code': '{71d93072-7ac3-4e86-92a8-8a394faf1fb8}'
}

base = 'console'

executables = [
    Executable('worker.py',
               base=base,
               target_name = 'minerva-worker',
               icon = 'assets/minerva.ico',
               copyright = 'Copyright (C) 2026 The Minerva Archive Contributors',
               shortcut_name = 'Minerva Worker',
               shortcut_dir = 'MinervaArchiveMenu'
               )
]

setup(name='Minerva Worker Standalone',
    version = '1.2.4',
    description = 'Minerva DPN Volunteer Download Client',
    options = {'build_exe': build_options, 'bdist_msi': msi_options},
    executables = executables
)
