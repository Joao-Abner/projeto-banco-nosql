import pandas as pd
from pymongo import MongoClient
import json
import os
import matplotlib.pyplot as plt
import seaborn as sns

# --- INSTRUÇÕES DE EXECUÇÃO ---
# 1. Certifique-se de ter o Python instalado (versão 3.x recomendada).

# 2. Instale o MongoDB Community Server no seu sistema operacional.
#    Disponível em: https://www.mongodb.com/try/download/community
#    Após a instalação, certifique-se de que o serviço do MongoDB está em execução.
#    (No Windows, geralmente é um serviço que inicia automaticamente).

# 3. Instale as bibliotecas Python necessárias via pip:
#    pip install pymongo pandas matplotlib seaborn jupyter

# 4. Baixe o dataset 'Universal Product Code Database' do Kaggle:
#    https://www.kaggle.com/datasets/rtatman/universal-product-code-database
#    Extraia o arquivo CSV (geralmente 'products.csv') e coloque-o na mesma pasta
#    deste script Python. Se o nome do arquivo for diferente, ajuste a variável CSV_FILE_PATH.

# 5. Execute este script Python em um ambiente como Jupyter Notebook ou diretamente via terminal: python seu_script.py
 
# 6. Para limpar o banco de dados e recomeçar a importação, basta executar o script novamente.

# --- CONFIGURAÇÃO DO MONGO DB ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "patrimonio_db" 
COLLECTION_NAME = "bens" 
CSV_FILE_PATH = "products.csv" 

def conectar_mongodb():
    """
    Estabelece a conexão com o MongoDB.
    Retorna o objeto da coleção.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        # O comando client.admin.command('ping') é uma forma de verificar a conexão.
        client.admin.command('ping')
        print(f"Conectado ao MongoDB: Banco '{DB_NAME}', Coleção '{COLLECTION_NAME}'.")
        return collection
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB. Verifique se o MongoDB está em execução em '{MONGO_URI}': {e}")
        return None
    
def importar_csv_para_mongodb(collection, csv_path, num_registros=5000):
    """
    Importa dados de um arquivo CSV para uma coleção MongoDB.
    Limita o número de registros para manter a simplicidade do projeto e o prazo.
    Mapeia campos do dataset para o modelo de bens patrimoniais.
    """
    if not os.path.exists(csv_path):
        print(f"Erro: Arquivo CSV '{csv_path}' não encontrado. Por favor, verifique o caminho do arquivo.")
        return
    
    try:
        # Carrega o CSV usando pandas. low_memory=False para evitar warnings com tipos de dados mistos em grandes CSVs.
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"CSV '{csv_path}' carregado. Total de registros: {len(df)}.")

        # Seleciona uma amostra dos dados (num_registros) para manter a simplicidade e agilidade.
        # Isso é crucial para o prazo de 2-3 semanas.
        if len(df) > num_registros:
            df_sample = df.head(num_registros)
            print(f"Processando os primeiros {num_registros} registros do CSV.")
        else:
            df_sample = df
            print(f"Processando todos os {len(df)} registros do CSV.") 

        # Converte o DataFrame para uma lista de dicionários (documentos JSON).
        # Adaptamos os campos do dataset original para o contexto de bens patrimoniais.
        # 'product_id' se torna 'asset_id' (simulando código de barras).
        # Adicionamos campos para 'location', 'status' e 'movement_history' para simular o TCC.
        documents = []
        for index, row in df_sample.iterrows():
            doc = {
                # Mapeia 'product_id' para 'asset_id' para simular o código de barras
                "asset_id": str(row.get("product_id", "")),
                "asset_name": str(row.get("product_name", "")),
                "category": str(row.get("category", "")),
                "brand": str(row.get("brand", "")),
                "description": str(row.get("description", "")),
                "location": "Armazenagem Principal", # Valor inicial para localização do bem
                "status": "Em Uso", # Valor inicial para status do bem
                "acquisition_date": pd.to_datetime('2023-01-01').strftime('%Y-%m-%d'), # Data de aquisição simulada
                "movement_history": [ # Histórico de movimentações (começa com a aquisição)
                    {
                        "date": pd.to_datetime('2023-01-01 00:00:00').strftime('%Y-%m-%d %H:%M:%S'),
                        "type": "Aquisição",
                        "from_location": "Fornecedor",
                        "to_location": "Armazenagem Principal",
                        "responsible": "Sistema Inicial"
                    }
                ]
            }
            documents.append(doc)
        # Insere os documentos no MongoDB.
        if documents:
            # Deletar documentos existentes para garantir uma importação limpa a cada execução
            collection.delete_many({})
            result = collection.insert_many(documents)
            print(f"{len(result.inserted_ids)} documentos importados com sucesso para o MongoDB.")
        else:
            print("Nenhum documento válido para importar.")

    except Exception as e:
        print(f"Erro ao importar CSV para MongoDB: {e}")

def apresentar_dados_como_dataframe(collection, limit=10):
    """
    Busca documentos da coleção MongoDB e os apresenta como um DataFrame do Pandas.
    Útil para verificar os dados após a importação.
    """
    try:
        cursor = collection.find().limit(limit)
        data = list(cursor)

        if data:
            df = pd.DataFrame(data)
            # Remove a coluna '_id' que é gerada automaticamente pelo MongoDB, pois não é necessária para a análise.
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])
            print(f"\n--- Primeiros {len(df)} documentos da coleção '{COLLECTION_NAME}' como DataFrame ---")
            # Usar .to_string() para exibir o DataFrame completo sem truncamento no console.
            print(df.to_string())
            return df
        else:
            print(f"Nenhum documento encontrado na coleção '{COLLECTION_NAME}'.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Erro ao apresentar dados como DataFrame: {e}")
        return pd.DataFrame()

def inserir_documento_extra(collection, num_docs=5):
    """
    Insere 'num_docs' documentos de dados extras na coleção.
    Simula a inserção manual de novos bens.
    """
    new_assets = []
    for i in range(num_docs):
        asset_id_new = f"NEWASSET{collection.count_documents({}) + 1 + i}"
        new_asset = {
            "asset_id": asset_id_new,
            "asset_name": f"Item Teste {i+1}",
            "category": "Eletrônico",
            "brand": "Genérica",
            "description": f"Descrição do Item Teste {i+1} para demonstração.",
            "location": "Laboratório 101",
            "status": "Ativo",
            "acquisition_date": pd.to_datetime('2024-05-15').strftime('%Y-%m-%d'),
            "movement_history": [
                {
                    "date": pd.to_datetime('2024-05-15 10:00:00').strftime('%Y-%m-%d %H:%M:%S'),
                    "type": "Aquisição",
                    "from_location": "Fornecedor",
                    "to_location": "Laboratório 101",
                    "responsible": "Administrador"
                }
            ]
        }
        new_assets.append(new_asset)
    try:
        result = collection.insert_many(new_assets)
        print(f"\nInseridos {len(result.inserted_ids)} documentos extras na coleção.")
        return result.inserted_ids
    except Exception as e:
        print(f"Erro ao inserir documentos extras: {e}")
        return []
    
def editar_documento(collection, asset_id, new_location=None, new_status=None):
    """
    Edita a localização ou status de um bem, simulando uma movimentação.
    Adiciona a movimentação ao histórico.
    """
    update_fields = {}
    current_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    movement_entry = {
        "date": current_time,
        "type": "Movimentação",
        "responsible": "Usuário App"
    }

    if new_location:
        # Pega a localização atual para o histórico
        current_asset = collection.find_one({"asset_id": asset_id})
        old_location = current_asset.get("location", "Desconhecida") if current_asset else "Desconhecida"
        update_fields["location"] = new_location
        movement_entry["from_location"] = old_location
        movement_entry["to_location"] = new_location
        print(f"Atualizando localização do bem '{asset_id}' de '{old_location}' para '{new_location}'.")

    if new_status:
        # Pega o status atual para o histórico
        current_asset = collection.find_one({"asset_id": asset_id})
        old_status = current_asset.get("status", "Desconhecido") if current_asset else "Desconhecido"
        update_fields["status"] = new_status
        # Adapta o tipo de movimentação se o status mudar
        if not new_location: # Se só o status mudar, não é uma movimentação de localização
             movement_entry["type"] = f"Atualização de Status: {old_status} para {new_status}"
             movement_entry["from_location"] = current_asset.get("location", "Desconhecida") if current_asset else "Desconhecida" # Mantém localização
             movement_entry["to_location"] = current_asset.get("location", "Desconhecida") if current_asset else "Desconhecida" # Mantém localização

        print(f"Atualizando status do bem '{asset_id}' de '{old_status}' para '{new_status}'.")

    try:
        # Atualiza os campos e adiciona a nova entrada ao array de 'movement_history'
        result = collection.update_one(
            {"asset_id": asset_id},
            {
                "$set": update_fields,
                "$push": {"movement_history": movement_entry}
            }
        )
        if result.modified_count > 0:
            print(f"Bem '{asset_id}' editado com sucesso e histórico de movimentação atualizado.")
        else:
            print(f"Bem '{asset_id}' não encontrado ou nenhum dado para editar.")
    except Exception as e:
        print(f"Erro ao editar documento: {e}")

def buscar_todos_documentos(collection):
    """
    Busca e retorna todos os documentos de uma coleção.
    """
    print(f"\n--- Todos os documentos na coleção '{COLLECTION_NAME}' ---")
    try:
        documents = list(collection.find({}))
        if documents:
            for doc in documents:
                print(doc)
            return documents
        else:
            print("Nenhum documento encontrado.")
            return []
    except Exception as e:
        print(f"Erro ao buscar todos os documentos: {e}")
        return []

def excluir_documento(collection, asset_id):
    """
    Exclui um documento da coleção pelo seu asset_id.
    """
    try:
        result = collection.delete_one({"asset_id": asset_id})
        if result.deleted_count > 0:
            print(f"\nDocumento com asset_id '{asset_id}' excluído com sucesso.")
        else:
            print(f"\nDocumento com asset_id '{asset_id}' não encontrado para exclusão.")
    except Exception as e:
        print(f"Erro ao excluir documento: {e}")

# --- CONSULTAS ÚTEIS PARA INVENTÁRIO (Item 4) ---
def buscar_bens_por_localizacao(collection, location):
    """
    Busca bens por uma localização específica.
    [cite_start]Operador: $eq (implícito) [cite: 21]
    """
    print(f"\n--- Bens na localização: '{location}' ---")
    try:
        assets = list(collection.find({"location": location}))
        if assets:
            for asset in assets:
                print(f"ID: {asset['asset_id']}, Nome: {asset['asset_name']}, Status: {asset['status']}")
            return assets
        else:
            print(f"Nenhum bem encontrado na localização '{location}'.")
            return []
    except Exception as e:
        print(f"Erro ao buscar bens por localização: {e}")
        return []
    
def buscar_bens_com_status_ou_categoria(collection, status=None, category=None):
    """
    Busca bens com um determinado status OU uma determinada categoria.
    [cite_start]Operador: $or [cite: 21]
    """
    query = {}
    if status and category:
        query = {"$or": [{"status": status}, {"category": category}]}
        print(f"\n--- Bens com status '{status}' OU categoria '{category}' ---")
    elif status:
        query = {"status": status}
        print(f"\n--- Bens com status '{status}' ---")
    elif category:
        query = {"category": category}
        print(f"\n--- Bens com categoria '{category}' ---")
    else:
        print("Forneça um status ou uma categoria para a busca.")
        return []

    try:
        assets = list(collection.find(query))
        if assets:
            for asset in assets:
                print(f"ID: {asset['asset_id']}, Nome: {asset['asset_name']}, Localização: {asset['location']}, Status: {asset['status']}, Categoria: {asset['category']}")
            return assets
        else:
            print("Nenhum bem encontrado com os critérios especificados.")
            return []
    except Exception as e:
        print(f"Erro ao buscar bens por status ou categoria: {e}")
        return []
    
def buscar_historico_de_movimentacao(collection, asset_id):
    """
    Busca o histórico de movimentação de um bem específico.
    """
    print(f"\n--- Histórico de Movimentação para o Bem: '{asset_id}' ---")
    try:
        asset = collection.find_one({"asset_id": asset_id}, {"asset_name": 1, "movement_history": 1})
        if asset and "movement_history" in asset:
            print(f"Nome do Bem: {asset.get('asset_name', 'Não informado')}")
            for entry in asset["movement_history"]:
                print(f"  - Data: {entry.get('date')}, Tipo: {entry.get('type')}, De: {entry.get('from_location')}, Para: {entry.get('to_location')}, Responsável: {entry.get('responsible')}")
            return asset["movement_history"]
        else:
            print(f"Bem '{asset_id}' não encontrado ou sem histórico de movimentação.")
            return []
    except Exception as e:
        print(f"Erro ao buscar histórico de movimentação: {e}")
        return []
    
# --- FUNÇÕES DE AGREGAÇÃO (Item 5) ---
def contar_bens_por_localizacao(collection):
    """
    [cite_start]Utiliza agregação para contar o número de bens por localização. [cite: 22]
    Agregação: $group, $sum
    """
    print("\n--- Contagem de Bens por Localização (Agregação) ---")
    try:
        pipeline = [
            {"$group": {"_id": "$location", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}} # Opcional: ordenar pela contagem decrescente
        ]
        results = list(collection.aggregate(pipeline))
        if results:
            for result in results:
                print(f"Localização: {result['_id']}, Quantidade: {result['count']}")
            return results
        else:
            print("Nenhuma contagem por localização encontrada.")
            return []
    except Exception as e:
        print(f"Erro ao contar bens por localização: {e}")
        return []

def contar_bens_por_categoria_e_status(collection):
    """
    [cite_start]Utiliza agregação para contar o número de bens por categoria e status. [cite: 22]
    Agregação: $group
    """
    print("\n--- Contagem de Bens por Categoria e Status (Agregação) ---")
    try:
        pipeline = [
            {"$group": {"_id": {"category": "$category", "status": "$status"}, "count": {"$sum": 1}}},
            {"$sort": {"_id.category": 1, "count": -1}} # Opcional: ordenar
        ]
        results = list(collection.aggregate(pipeline))
        if results:
            for result in results:
                print(f"Categoria: {result['_id']['category']}, Status: {result['_id']['status']}, Quantidade: {result['count']}")
            return results
        else:
            print("Nenhuma contagem por categoria e status encontrada.")
            return []
    except Exception as e:
        print(f"Erro ao contar bens por categoria e status: {e}")
        return []

# --- ANÁLISE EXPLORATÓRIA DOS DADOS VIA PANDAS (Item 6) ---
def analise_exploratoria(collection):
    """
    [cite_start]Realiza uma análise exploratória dos dados da coleção usando Pandas. [cite: 23]
    [cite_start]Apresenta distribuição de frequência e visão geral de tipos e métricas. [cite: 23, 24]
    """
    print("\n--- Análise Exploratória de Dados com Pandas ---")
    try:
        # Busca todos os documentos para a análise exploratória (pode ser limitado para grandes datasets)
        data = list(collection.find({}, {"_id": 0})) # Exclui o _id para o DataFrame
        if not data:
            print("Nenhum dado para análise exploratória.")
            return pd.DataFrame()

        df = pd.DataFrame(data)

        print("\nVisão Geral do DataFrame (Head):")
        print(df.head())

        print("\nInformações do DataFrame (Tipos de Dados e Não-Nulos):")
        df.info()

        # [cite_start]a. Distribuição de frequência de um documento e campo (ex: 'location') [cite: 23]
        print("\n--- Distribuição de Frequência da Localização (df['location'].value_counts()) ---")
        location_counts = df['location'].value_counts()
        print(location_counts)

        # b. [cite_start]Visão geral dos tipos e métricas (avg, std, quartis, entre outros) de um documento. [cite: 24]
        # Para isso, precisamos de campos numéricos. Se o dataset não tiver, usaremos a contagem
        # de itens por categoria/localização como exemplo.
        print("\n--- Métricas Descritivas de Campos Numéricos (se existirem) ---")
        numeric_cols = df.select_dtypes(include=['number']).columns
        if not numeric_cols.empty:
            print(df[numeric_cols].describe())
        else:
            print("Não há colunas numéricas diretas no DataFrame para 'describe()'.")
            print("\nExemplo de métricas para contagem de bens por categoria:")
            category_counts = df['category'].value_counts()
            print(category_counts.describe())


        return df
    except Exception as e:
        print(f"Erro durante a análise exploratória: {e}")
        return pd.DataFrame()

# --- GERAÇÃO DE GRÁFICOS (Item 7) ---
def gerar_graficos(df):
    """
    [cite_start]Gera dois gráficos diferentes a partir do DataFrame e apresenta os dados pertinentes. [cite: 25]
    """
    if df.empty:
        print("DataFrame vazio, não é possível gerar gráficos.")
        return

    print("\n--- Gerando Relatórios Visuais ---")

    # Gráfico 1: Quantidade de Bens por Localização (Gráfico de Barras)
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df, y='location', order=df['location'].value_counts().index, palette='viridis')
    plt.title('Distribuição de Bens por Localização')
    plt.xlabel('Quantidade de Bens')
    plt.ylabel('Localização')
    plt.tight_layout()
    plt.savefig('bens_por_localizacao.png') # Salva o gráfico como imagem
    print("Gráfico 'bens_por_localizacao.png' gerado com sucesso.")
    plt.show() # Exibe o gráfico

    # Gráfico 2: Distribuição de Bens por Categoria (Gráfico de Pizza)
    # Selecionar as 10 categorias mais comuns para evitar um gráfico muito poluído
    top_categories = df['category'].value_counts().nlargest(10)
    if not top_categories.empty:
        plt.figure(figsize=(10, 10))
        plt.pie(top_categories, labels=top_categories.index, autopct='%1.1f%%', startangle=90, cmap='Pastel1')
        plt.title('Distribuição Percentual de Bens por Categoria (Top 10)')
        plt.axis('equal') # Garante que o gráfico de pizza seja um círculo.
        plt.tight_layout()
        plt.savefig('bens_por_categoria_pizza.png') # Salva o gráfico como imagem
        print("Gráfico 'bens_por_categoria_pizza.png' gerado com sucesso.")
        plt.show() # Exibe o gráfico
    else:
        print("Não há categorias suficientes para gerar o gráfico de pizza.")

# --- EXECUÇÃO PRINCIPAL DO PROJETO ---
if __name__ == "__main__":
    print("--- INICIANDO PROJETO FINAL: SISTEMA DE RASTREAMENTO DE BENS PATRIMONIAIS ---")

    # [cite_start]1. Conectar ao MongoDB [cite: 14]
    bens_collection = conectar_mongodb()

    if bens_collection is not None:
        # [cite_start]Importar o dataset CSV para o MongoDB. [cite: 16] A função já lida com a limpeza da coleção antes da importação.
        importar_csv_para_mongodb(bens_collection, CSV_FILE_PATH, num_registros=5000)

        # [cite_start]Apresentar os dados importados como DataFrame. [cite: 17]
        df_patrimonio = apresentar_dados_como_dataframe(bens_collection, limit=20)

        # -------------------------------------------------------------
        # --- IMPLEMENTAÇÃO DOS ITENS OBRIGATÓRIOS DO TRABALHO ---
        # -------------------------------------------------------------

        # [cite_start]Item 3a: Apresentar a coleção ou coleções existente(s) no banco de dados [cite: 18]
        print("\n--- Coleções existentes no banco de dados 'patrimonio_db' ---")
        try:
            db_client = MongoClient(MONGO_URI)
            database = db_client[DB_NAME]
            print(database.list_collection_names())
            db_client.close()
        except Exception as e:
            print(f"Erro ao listar coleções: {e}")

        # [cite_start]Item 3b: Em cada coleção, insira 5 documentos de dados extras. [cite: 19]
        inserted_ids = inserir_documento_extra(bens_collection, num_docs=5)

        # [cite_start]Item 3c: Editar 2 opções diferentes de dados (independente do documento). [cite: 19]
        # Para isso, precisamos de alguns asset_ids existentes ou recém-inseridos.
        # Vamos pegar um dos assets do dataset original e um dos recém-inseridos.
        # Primeiro, pegamos um asset_id do banco para editar.
        sample_asset = bens_collection.find_one({"asset_name": {"$ne": "Item Teste 1"}}) # Pega um que não seja o de teste
        if sample_asset:
            asset_to_edit_1 = sample_asset['asset_id']
            editar_documento(bens_collection, asset_to_edit_1, new_location="Reitoria", new_status="Em Manutenção")
        else:
            print("Não foi possível encontrar um asset existente para editar. Considere inserir mais dados.")

        if inserted_ids: # Se houver IDs de documentos recém-inseridos
            asset_to_edit_2 = bens_collection.find_one({"_id": inserted_ids[0]})['asset_id'] # Pega o primeiro ID inserido
            editar_documento(bens_collection, asset_to_edit_2, new_status="Desativado")
        else:
            print("Não foi possível encontrar um asset recém-inserido para editar. Considere inserir mais dados.")


        # [cite_start]Item 3d: Permitir realizar a busca por todos os documentos de cada coleção [cite: 20]
        buscar_todos_documentos(bens_collection)

        # [cite_start]Item 3e: Excluir 2 documentos em cada coleção. [cite: 20]
        # Excluir os dois primeiros documentos que foram inseridos como extras para demonstração.
        if inserted_ids and len(inserted_ids) >= 2:
            excluir_documento(bens_collection, bens_collection.find_one({"_id": inserted_ids[0]})['asset_id'])
            excluir_documento(bens_collection, bens_collection.find_one({"_id": inserted_ids[1]})['asset_id'])
        else:
            print("Não há documentos extras suficientes para exclusão de 2 documentos.")
            # Opcional: Se não houver extras, excluir dois documentos aleatórios do dataset principal.
            # print("Excluindo dois documentos aleatórios do dataset principal...")
            # sample_for_deletion = list(bens_collection.aggregate([{"$sample": {"size": 2}}]))
            # if sample_for_deletion:
            #     for doc in sample_for_deletion:
            #         excluir_documento(bens_collection, doc['asset_id'])

        # [cite_start]Item 4: Permitir o uso de pelo menos 2 buscas utilizando diferentes operadores. [cite: 21]
        # Busca 1: Bens em uma localização específica (operador implícito $eq)
        buscar_bens_por_localizacao(bens_collection, "Laboratório 101")
        # Busca 2: Bens com status 'Em Manutenção' OU categoria 'Eletrônico' (operador $or)
        buscar_bens_com_status_ou_categoria(bens_collection, status="Em Manutenção", category="Eletrônico")
        # Busca 3: Histórico de movimentação de um bem específico (busca por ID)
        if sample_asset:
             buscar_historico_de_movimentacao(bens_collection, sample_asset['asset_id'])


        # [cite_start]Item 5: Permitir o uso de pelo menos 2 buscas utilizando diferentes funções de agregação. [cite: 22]
        contar_bens_por_localizacao(bens_collection)
        contar_bens_por_categoria_e_status(bens_collection)

        # [cite_start]Item 6: Faça uma análise exploratória dos dados via pandas e apresente. [cite: 23]
        df_full_data = analise_exploratoria(bens_collection)

        # [cite_start]Item 7: Monte 2 gráficos diferentes e apresente os dados que achar pertinente. [cite: 25]
        gerar_graficos(df_full_data)

        print("\n--- PROJETO FINAL CONCLUÍDO ---")
        print("Verifique os arquivos PNG gerados para os relatórios visuais.")
        print("Você pode inspecionar o banco de dados 'patrimonio_db' usando o MongoDB Compass para ver as alterações.")