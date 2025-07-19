import os

try:
    import sqlalchemy_dremio.flight
    print("✅ Driver Dremio Flight carregado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao carregar driver Dremio Flight: {e}")

try:
    import sqlalchemy_dremio.odbc
    print("✅ Driver Dremio ODBC carregado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao carregar driver Dremio ODBC: {e}")

# habilita proxy fix para Docker
ENABLE_PROXY_FIX = True