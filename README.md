# 🤖 LinkedIn Job Search Engine: Automated with Python & GenAI

![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python)
![Gemini](https://img.shields.io/badge/Google_Gemini-2.0_Flash-orange?style=for-the-badge&logo=google)
![Google Sheets](https://img.shields.io/badge/Google_Sheets-API-green?style=for-the-badge&logo=googlesheets)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-Automated-2088FF?style=for-the-badge&logo=githubactions)

## 🇺🇸 English

**LinkedIn Job Searching Automated With Python and AI.**

An autonomous job search ETL pipeline built with Python and Artificial Intelligence, designed exclusively for **LinkedIn**. 

It uses a specialized Job Search API to fetch recent remote job listings and the **Google Gemini 2.0 API** to analyze complex descriptions, filter out noise (such as "fake remote" jobs and sponsored outdated ads), and extract core requirements (tech stack, estimated salary, and hierarchical level) based on custom criteria. The final structured dataset is automatically synced to a Google Sheets database.

### ⚙️ Core Features
* **Targeted Extraction:** Fetches 100% remote jobs posted within the last 24 hours directly from LinkedIn.
* **Double-Layer Filtration:** Uses Python scripts to block bad titles/outdated ads, and GenAI to audit the *real* workplace model (catching sponsored in-office ads disguised as remote).
* **AI Enrichment:** Extracts Hard Skills, Soft Skills, and Tools (e.g., Power BI, Fabric, SQL, Python) from unstructured text.
* **Cloud Automation:** Runs fully autonomously via GitHub Actions, securing API keys through GitHub Secrets.

---

## 🇧🇷 Português

**Busca de Vagas no LinkedIn Automatizada com Python e IA.**

Um pipeline ETL autônomo de busca de vagas construído com Python e Inteligência Artificial, focado exclusivamente no **LinkedIn**.

Ele utiliza uma API especializada em buscas do LinkedIn para capturar vagas remotas recentes e a API do **Google Gemini 2.0** para analisar descrições complexas, filtrar ruídos (como falsas vagas remotas e anúncios patrocinados antigos) e extrair requisitos essenciais (stack tecnológico, estimativa de salário e nível hierárquico) com base em critérios personalizados. O conjunto de dados estruturado é sincronizado automaticamente em um banco de dados no Google Sheets.

### ⚙️ Funcionalidades Principais
* **Extração Direcionada:** Captura vagas 100% remotas publicadas nas últimas 24 horas diretamente do LinkedIn.
* **Filtro de Camada Dupla:** Utiliza scripts Python para barrar títulos ruins/vagas antigas, e IA Generativa para auditar o modelo de trabalho *real* (bloqueando anúncios presenciais disfarçados de remoto).
* **Enriquecimento com IA:** Extrai Hard Skills, Soft Skills e Ferramentas (ex: Power BI, Fabric, SQL, Python) de textos não estruturados.
* **Automação em Nuvem:** Roda de forma totalmente autônoma via GitHub Actions, com chaves de API protegidas nativamente pelo GitHub Secrets.
