"""
==============================================================================
🤖 LINKEDIN SERCH JOBS ENGINE V1.0 - ETL PIPELINE WITH GENAI (GEMINI 2.0)
==============================================================================
EN: This script fetches remote data jobs (Data Science, Analytics, BI), 
filters out fake remote and outdated ads using Python, enriches the data 
(tech stack, salary estimation) using Google Gemini 2.0 Flash, and 
syncs the structured dataset directly to Google Sheets.

PT: Este script busca vagas remotas na área de Dados, filtra oportunidades 
reais (removendo falsos remotos e vagas antigas) usando Python e IA Generativa, 
e salva os resultados estruturados em um banco de dados no Google Sheets.
==============================================================================
"""

import requests
import pandas as pd
import time
import re
import json
import os
from datetime import datetime
from pydantic import BaseModel, Field
from google import genai
from google.genai import types
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================================================================
# 1. CONFIGURAÇÕES E CHAVES (VIA GITHUB SECRETS)
# ==============================================================================
print("⏳ Iniciando o Motor de Vagas de Dados...")

NOME_PLANILHA_GOOGLE = "BD_DATA_JOBS"
META_TOTAL_GLOBAL = 400
META_POR_TERMO = 80

TERMOS_BUSCA = [
    "Dados", "Data", "Analytics", "Inteligência de Negócio",
    "Cientista de Dados", "Business Intelligence", "Power BI", "Fabric"
]

try:
    # Lendo chaves de ambiente (Injetadas pelo GitHub Actions)
    RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY')
    GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
    CREDENCIAIS_JSON = os.environ.get('CREDENCIAIS_JSON')

    if not all([RAPIDAPI_KEY, GOOGLE_API_KEY, CREDENCIAIS_JSON]):
        raise ValueError("Chaves secretas não encontradas no ambiente.")
except Exception as e:
    print(f"❌ ERRO nas Chaves: {e}")
    exit(1)

headers_rapid = {
    "x-rapidapi-host": "jobs-api14.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY
}

client_genai = genai.Client(api_key=GOOGLE_API_KEY)

# ==============================================================================
# 2. SCHEMA DA IA
# ==============================================================================
class DadosMercadoData(BaseModel):
    localidade: str = Field(description="Cidade ou Estado da vaga.")
    modelo_trabalho: str = Field(description="Classifique como: 'Presencial', 'Híbrido', 'Remoto' ou 'Não Informado'.")
    categoria_hierarquica: str = Field(description="Ex: 'Especialista/Sênior', 'Pleno/Técnico', 'Júnior/Operacional'.")
    ferramentas_exigidas: list[str] = Field(description="Ex: 'Python', 'SQL', 'Power BI', 'Fabric', etc.")
    salario_na_descricao: str | None = Field(description="Valor explícito se houver.")
    salario_estimado_ia: str = Field(description="Estimativa de mercado.")
    hard_skills: list[str] = Field(description="Competências técnicas.")
    soft_skills: list[str] = Field(description="Competências comportamentais.")

# ==============================================================================
# 3. ETAPA A: EXTRAÇÃO (API LINKEDIN)
# ==============================================================================
vagas_unicas = {}
termos_pesquisados = []
print(f"\n🔎 ETAPA A: Varredura no LinkedIn (Meta: {META_TOTAL_GLOBAL} vagas)...")

for termo in TERMOS_BUSCA:
    if len(vagas_unicas) >= META_TOTAL_GLOBAL: break
    termos_pesquisados.append(termo)
    print(f"   Buscando: '{termo}'... ", end="")

    next_token = None
    count_termo = 0

    while count_termo < META_POR_TERMO:
        try:
            params = {
                "query": termo, "location": "Brazil", "employmentTypes": "fulltime",
                "datePosted": "day", "workplaceTypes": "remote"
            }
            if next_token: params["token"] = next_token

            time.sleep(1.5)
            resp = requests.get("https://jobs-api14.p.rapidapi.com/v2/linkedin/search", headers=headers_rapid, params=params).json()

            if 'data' not in resp or not resp['data']: break
            for v in resp['data']:
                if v['id'] not in vagas_unicas: vagas_unicas[v['id']] = v

            count_termo += len(resp['data'])
            if len(vagas_unicas) >= META_TOTAL_GLOBAL: break
            
            next_token = resp.get('meta', {}).get('nextToken')
            if not next_token: break
        except: break

    print(f"[{count_termo} encontradas brutas]")

ids_vagas = list(vagas_unicas.values())[:META_TOTAL_GLOBAL]
print(f"✅ IDs encontrados. Baixando detalhes de {len(ids_vagas)} vagas...")

dados_brutos = []
for i, vaga in enumerate(ids_vagas):
    print(f"\r   Baixando [{i+1}/{len(ids_vagas)}]...", end="")
    try:
        time.sleep(1.2)
        r = requests.get("https://jobs-api14.p.rapidapi.com/v2/linkedin/get", headers=headers_rapid, params={"id": vaga.get('id')})
        if r.status_code == 200:
            detalhe = r.json().get('data', {})
            desc = detalhe.get('description', '')
            if desc and len(str(desc)) > 50:
                dados_brutos.append({
                    "id": vaga.get('id'), "titulo": vaga.get('title'), "empresa": vaga.get('companyName'),
                    "localidade_api": vaga.get('location', 'Não Informado'), "data_publicacao": vaga.get('datePosted') or vaga.get('postedTimeAgo'),
                    "link": detalhe.get('linkedinUrl'), "descricao_completa": desc
                })
    except: pass

df_bruto = pd.DataFrame(dados_brutos)

# ==============================================================================
# 4. ETAPA B: TRANSFORMAÇÃO E FILTRO (PYTHON)
# ==============================================================================
df_aprovado = pd.DataFrame()
df_reprovado = pd.DataFrame()

if not df_bruto.empty:
    print(f"\n\n🧹 ETAPA B: Filtrando vagas válidas (Título e Tempo)...")
    
    termos_aceitos = ['data', 'dados', 'analytics', 'bi', 'business intelligence', 'data analyst', 'fabric', 'powerbi', 'power bi', 'data specialist', 'inteligência de negócio', 'engenharia de dados', 'cientista de dados', 'engineer', 'scientist']
    mask_titulo = df_bruto['titulo'].str.contains('|'.join(termos_aceitos), case=False, na=False)

    termos_velhos = ['semana', 'mês', 'meses', 'ano', 'week', 'month', 'year', '2 dias', '3 dias', '4 dias', '5 dias', '6 dias', '2 days', '3 days', '4 days', '5 days', '6 days']
    mask_tempo = ~df_bruto['data_publicacao'].astype(str).str.contains('|'.join(termos_velhos), case=False, na=False)

    mask_final = mask_titulo & mask_tempo
    df_aprovado = df_bruto[mask_final].copy()
    df_reprovado = df_bruto[~mask_final].copy()

    print(f"✅ Vagas aprovadas (Boas e Recentes): {len(df_aprovado)} | 🗑️ Descartadas: {len(df_reprovado)}")

# ==============================================================================
# 5. ETAPA C: ENRIQUECIMENTO COM IA (GEMINI)
# ==============================================================================
if not df_aprovado.empty:
    print(f"\n🧠 ETAPA C: Processando IA do Google Gemini nas aprovadas...")
    dados_processados = []
    lista_aprovadas = df_aprovado.to_dict('records')

    for i, row in enumerate(lista_aprovadas):
        print(f"\r   Analisando vaga [{i+1}/{len(lista_aprovadas)}] | IA Sucessos: {len(dados_processados)}...", end="")
        try:
            prompt = f"Analise vaga de Dados. Local: '{row.get('localidade_api', '')}'. Extraia as ferramentas (Stack). Identifique se é Presencial, Híbrido ou Remoto. Salário: Estime. Vaga: {row.get('descricao_completa', '')}"
            response = client_genai.models.generate_content(
                model='gemini-2.0-flash', contents=prompt,
                config=types.GenerateContentConfig(response_mime_type='application/json', response_schema=DadosMercadoData)
            )
            insights = response.parsed.model_dump()

            for k in ['ferramentas_exigidas', 'hard_skills', 'soft_skills']:
                if isinstance(insights.get(k), list): insights[k] = ", ".join(insights[k])

            registro = row.copy()
            registro.update(insights)
            if 'descricao_completa' in registro: del registro['descricao_completa']
            dados_processados.append(registro)
            
            time.sleep(4.5)
        except Exception as e:
            time.sleep(10)
            pass

# ==============================================================================
# 6. ETAPA D: CARGA E AUDITORIA NO SHEETS
# ==============================================================================
if 'dados_processados' in locals() and dados_processados:
    print("\n\n⚙️ ETAPA D: Ajustes finais e Sincronização...")
    df_ia = pd.DataFrame(dados_processados)

    mask_remoto = df_ia['modelo_trabalho'].str.upper() == 'REMOTO'
    df_final = df_ia[mask_remoto].copy()
    df_falsos_remotos = df_ia[~mask_remoto].copy()

    print(f"🕵️ Auditoria da IA: {len(df_final)} remotas reais | {len(df_falsos_remotos)} falsas remotas barradas.")

    if not df_final.empty:
        df_final['salario_medio'] = df_final['salario_estimado_ia'].apply(lambda x: sum([v * 1000 if v < 100 else v for v in [float(n) for n in re.findall(r'\d+', str(x).replace('.', ''))]]) / len([float(n) for n in re.findall(r'\d+', str(x).replace('.', ''))]) if pd.notna(x) and re.findall(r'\d+', str(x).replace('.', '')) else None)
        df_final['categoria_tratada'] = df_final['categoria_hierarquica'].replace({'Técnico/Pl-Sr': 'Técnico/Especialista', 'Analista': 'Técnico/Especialista', 'Operacional/Jr': 'Júnior/Entrada', 'Assistente': 'Júnior/Entrada', 'Gestão': 'Gestão/Liderança', 'Executivo': 'Diretoria/C-Level'})
        
        def limpar_localidade(loc):
            if pd.isna(loc): return "Não Informado"
            loc = str(loc).replace(", Brazil", "").replace(" Brazil", "").replace("Greater ", "").replace(" Area", "").strip()
            return "Brasil (Remoto / Nacional)" if loc == "Brazil" else loc
            
        df_final['localidade'] = df_final['localidade_api' if 'localidade_api' in df_final.columns else 'localidade'].apply(limpar_localidade)
        df_final['regiao'] = 'Remoto / Nacional'

    print("☁️ Conectando ao Google Sheets...")
    try:
        creds_dict = json.loads(os.environ.get('CREDENCIAIS_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        planilha_mestre = gspread.authorize(creds).open(NOME_PLANILHA_GOOGLE)
        hoje = datetime.now().strftime("%Y-%m-%d")

        if not df_final.empty:
            sheet_aprovadas = planilha_mestre.sheet1
            ids_existentes = [str(r['id']) for r in sheet_aprovadas.get_all_records()] if sheet_aprovadas.get_all_records() else []
            novas_vagas = []

            if not ids_existentes:
                sheet_aprovadas.append_row(df_final.columns.tolist() + ['data_coleta_sistema', 'status_sistema'])

            for _, row in df_final.iterrows():
                if str(row['id']) not in ids_existentes:
                    linha = ["" if pd.isna(i) or i == float('inf') or i == float('-inf') else i if isinstance(i, (int, float)) else str(i) for i in row.tolist()]
                    linha.extend([hoje, "Nova"])
                    novas_vagas.append(linha)

            if novas_vagas:
                sheet_aprovadas.append_rows(novas_vagas)
                print(f"✅ SUCESSO! {len(novas_vagas)} vagas salvas na aba principal.")
            else:
                print("✅ Nenhuma vaga inédita hoje.")
                
        try:
            sheet_descartadas = planilha_mestre.worksheet("Descartadas")
        except:
            sheet_descartadas = planilha_mestre.add_worksheet(title="Descartadas", rows="100", cols="20")

        ids_desc_existentes = [str(r['id']) for r in sheet_descartadas.get_all_records()] if sheet_descartadas.get_all_records() else []
        novas_descartadas = []
        colunas_base = df_reprovado.columns.tolist() if not df_reprovado.empty else ['id', 'titulo', 'empresa', 'localidade_api', 'data_publicacao', 'link']

        if not ids_desc_existentes:
            sheet_descartadas.append_row(colunas_base + ['data_coleta_sistema', 'motivo_descarte'])

        if not df_reprovado.empty:
            for _, row in df_reprovado.iterrows():
                if str(row['id']) not in ids_desc_existentes:
                    linha_desc = ["" if pd.isna(i) else i if isinstance(i, (int, float)) else str(i) for i in row.tolist()]
                    linha_desc.extend([hoje, "Reprovada pelo Filtro Python"])
                    novas_descartadas.append(linha_desc)
                    ids_desc_existentes.append(str(row['id']))

        if not df_falsos_remotos.empty:
            for _, row in df_falsos_remotos.iterrows():
                if str(row['id']) not in ids_desc_existentes:
                    linha_desc = [row.get(col, "") for col in colunas_base]
                    linha_desc = ["" if pd.isna(v) else v if isinstance(v, (int, float)) else str(v) for v in linha_desc]
                    linha_desc.extend([hoje, f"Falso Remoto detectado pela IA: {row.get('modelo_trabalho')}"])
                    novas_descartadas.append(linha_desc)
                    ids_desc_existentes.append(str(row['id']))

        if novas_descartadas:
            sheet_descartadas.append_rows(novas_descartadas)
            print(f"🗑️ SUCESSO! {len(novas_descartadas)} descartes registrados.")

    except Exception as e:
        print(f"❌ Erro no Sheets: {e}")
else:
    print("\n⚠️ Nenhuma vaga processada para sincronizar.")

print("\n=========================================================")
print(f"📊 RELATÓRIO FINAL: Busca finalizada.")
print("=========================================================")
