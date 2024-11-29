import pandas as pd
import re

# Carregar o arquivo com a Base de Dados
data = pd.read_csv(r'D:\Users\MD_SÁ\Downloads\Base de dados - Case DQA.csv')

# 1. Análise de Preenchimento
preenchimento_df = data.notnull().mean().reset_index()
preenchimento_df.columns = ['Coluna', 'Proporção de Preenchimento']

# Contabilizar a quantidade de campos não preenchidos
campos_nao_preenchidos = data.isnull().sum().reset_index()
campos_nao_preenchidos.columns = ['Coluna', 'Campos Não Preenchidos']

# Mesclando as duas informações em um DF
preenchimento_df = preenchimento_df.merge(campos_nao_preenchidos, on='Coluna')


# 2. Análise de Padronização para regrade de negocio

def validar_maiusculas_sem_acentos(coluna):
    """verifica se os valores em uma coluna estão dentro de um conjunto de categorias válidas, considerando apenas os valores não nulos"""
    return coluna.apply(lambda x: bool(re.match(r'^[A-Z\s]+$', str(x))) if pd.notnull(x) else False)

def validar_categorias_exatas(coluna, categorias_validas):
    """verificar se os valores em uma coluna estão dentro de um conjunto de atributos válidas"""
    return coluna.apply(lambda x: x in categorias_validas if pd.notnull(x) else False)

def validar_datas_individuais(coluna):
    """Verifica se os valores em uma coluna de datas seguem o formato AAAA-MM-DD."""
    return coluna.dropna().apply(lambda x: bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(x))))

def validar_relacao_datas(data_ajuizamento, data_sentenca):
    """Verifica se Data Ajuizamento é menor que Data da Sentença.Ignora os casos onde Data da Sentença é vazio ou nulo."""
    # Converter as colunas para datetime (ignorar erros)
    data_ajuizamento = pd.to_datetime(data_ajuizamento, errors='coerce')
    data_sentenca = pd.to_datetime(data_sentenca, errors='coerce')
    
    # Ignorar os casos onde "Data da Sentença" é nulo
    mask_not_null = data_sentenca.notnull()
    
    # Comparar as datas apenas para registros não válidos
    resultado = (data_ajuizamento > data_sentenca) & mask_not_null
    
    # Retornar os resultados  das datas DA maiores (sem alterar valores nulos)
    return (~resultado)

def validar_valores(coluna):
    return coluna.dropna().apply(lambda x: bool(re.match(r'^\d+(\.\d{1,2})?$', str(x))))

# Categorias válidas para colunas específicas
categorias_resultado_processo = ["ARQUIVADO", "INDEFINIDO", "IMPROCEDENTE", "PROCEDENTE"]
categorias_status = ["JULGADO", "EM ANDAMENTO"]
categorias_tipo_acao = ["ACAO PENAL", "ACAO CIVEL", "ACAO TRABALHISTA"]
categorias_motivo_acao = ["DIVORCIO", "INDENIZACAO", "DIVIDA"]
categorias_vara = ["1ª VARA CIVEL", "2ª VARA CIVEL", "3ª VARA CRIMINAL", "4ª VARA CIVEL", "5ª VARA CIVEL"]

# Validar todas as colunas mencionadas detro da regra de negocio
regras_validacao = {
    "Nome Autor": validar_maiusculas_sem_acentos(data["Nome Autor"]),
    "Nome Réu": validar_maiusculas_sem_acentos(data["Nome Réu"]),
    "Advogado Autor": validar_maiusculas_sem_acentos(data["Advogado Autor"]),
    "Advogado Réu": validar_maiusculas_sem_acentos(data["Advogado Réu"]),
    "Data Ajuizamento": validar_datas_individuais(data["Data Ajuizamento"]),
    "Data da Sentença": validar_datas_individuais(data["Data da Sentença"]),
    "Relação entre Datas": validar_relacao_datas(data["Data Ajuizamento"], data["Data da Sentença"]),
    "Valor da Causa": validar_valores(data["Valor da Causa"]),
    "Valor da Sentença": validar_valores(data["Valor da Sentença"]),
    "Tipo de Ação": validar_categorias_exatas(data["Tipo de Ação"], categorias_tipo_acao),
    "Motivo da Ação": validar_categorias_exatas(data["Motivo da Ação"], categorias_motivo_acao),
    "Resultado do Processo": validar_categorias_exatas(data["Resultado do Processo"], categorias_resultado_processo),
    "Status": validar_categorias_exatas(data["Status"], categorias_status),
    "Vara": validar_categorias_exatas(data["Vara"], categorias_vara),
}

# Calcular proporção e desvios encontrados
padronizacao_resultados = {}
padronizacao_desvios = {}

for coluna, resultado in regras_validacao.items():
    padronizacao_resultados[coluna] = resultado.mean()
    padronizacao_desvios[coluna] = (~resultado).sum()

# Criar DataFrame de padronização
padronizacao_df = pd.DataFrame({
    'Coluna': list(padronizacao_resultados.keys()),
    'Proporção Padronizada': list(padronizacao_resultados.values()),
    'Atributos Fora do Padrão': list(padronizacao_desvios.values())
})

# 3. Análise de Consistência // fazer direto no BI 
data_filtrada = data[data['Data da Sentença'].notnull()]
consistentes = (pd.to_datetime(data_filtrada['Data Ajuizamento'], errors='coerce') < 
                pd.to_datetime(data_filtrada['Data da Sentença'], errors='coerce')).sum()
total = len(data_filtrada)
consistencia_df = pd.DataFrame([['Datas Consistentes', consistentes, total - consistentes, consistentes / total]],
                               columns=['Regra', 'Dados Corretos', 'Dados Incorretos', 'Proporção Consistente'])

# 4. Análise de Unicidade, verificando apenas o ID e Numero de Processo
unicidade_resultados = []

for coluna in ['ID Processo', 'Número do Processo']:
    valores_unicos = data[coluna].nunique()
    duplicados = len(data) - valores_unicos
    unicidade_resultados.append([coluna, valores_unicos, duplicados])

unicidade_df = pd.DataFrame(unicidade_resultados, columns=['Coluna', 'Valores Únicos', 'Registros Duplicados'])

# 5. Análise de Abrangência feito encima apenas da culna VARA
abrangencia_df = data['Vara'].value_counts(normalize=True).reset_index()
abrangencia_df.columns = ['Vara', 'Proporção']

# Salvar os resultados garimpados em arquivos CSV
preenchimento_df.to_csv(r'D:\Users\MD_SÁ\Downloads\preenchimento.csv', index=False)
padronizacao_df.to_csv(r'D:\Users\MD_SÁ\Downloads\padronizacao.csv', index=False)
consistencia_df.to_csv(r'D:\Users\MD_SÁ\Downloads\consistencia.csv', index=False)
unicidade_df.to_csv(r'D:\Users\MD_SÁ\Downloads\unicidade.csv', index=False)
abrangencia_df.to_csv(r'D:\Users\MD_SÁ\Downloads\abrangencia.csv', index=False)

print("Análises salvas com sucesso:")
print("- preenchimento.csv")
print("- padronizacao.csv")
print("- consistencia.csv")
print("- unicidade.csv")
print("- abrangencia.csv")
