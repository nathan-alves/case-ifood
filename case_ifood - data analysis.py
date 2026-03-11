# Databricks notebook source
# MAGIC %md
# MAGIC # iFood Data Analysis Case
# MAGIC
# MAGIC ## Contexto
# MAGIC
# MAGIC O iFood realizou um experimento A/B oferecendo cupons para um grupo de usuários com o objetivo de aumentar retenção e crescimento.
# MAGIC
# MAGIC Usuários foram divididos em dois grupos:
# MAGIC
# MAGIC - **Control**: não receberam cupom
# MAGIC - **Target**: receberam cupom
# MAGIC
# MAGIC ## Objetivo da análise
# MAGIC
# MAGIC 1. Avaliar o impacto da campanha
# MAGIC 2. Verificar se houve aumento de pedidos e receita
# MAGIC 3. Avaliar a viabilidade financeira
# MAGIC 4. Identificar segmentos de usuários mais impactados
# MAGIC 5. Propor melhorias para a estratégia de cupons
# MAGIC
# MAGIC Este notebook foi estruturado para responder integralmente às questões do case, cobrindo preparação dos dados, análise do experimento A/B, viabilidade financeira, segmentação de usuários e recomendações de negócio.

# COMMAND ----------

# Bibliotecas

import pandas as pd
import numpy as np
import requests
import gzip
import json
import tarfile
import io
from collections import defaultdict
from scipy.stats import ttest_ind

# COMMAND ----------

# Leitura dos datasets

consumers = pd.read_csv(
    "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
    compression="gzip"
)

restaurants = pd.read_csv(
    "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
    compression="gzip"
)

# O arquivo do experimento está compactado em formato tar.gz. Por isso, o CSV interno é extraído antes da leitura
url = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"

response = requests.get(url)
response.raise_for_status()

tar_bytes = io.BytesIO(response.content)

with tarfile.open(fileobj=tar_bytes, mode="r:gz") as tar:
    target_member = [m for m in tar.getmembers() if m.name == "ab_test_ref.csv"][0]
    file = tar.extractfile(target_member)
    ab_test = pd.read_csv(file, encoding="latin-1")

# COMMAND ----------

# Validação dos datasets

print("consumers:", consumers.shape)
print("restaurants:", restaurants.shape)
print("ab_test:", ab_test.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estratégia de tratamento dos dados
# MAGIC
# MAGIC Os dados foram acessados a partir das URLs públicas disponibilizadas no case.
# MAGIC
# MAGIC O arquivo de pedidos foi processado por **streaming**, linha a linha, para evitar carregar milhões de registros em memória ao mesmo tempo.
# MAGIC
# MAGIC A base analítica foi construída no nível de **usuário**, pois a randomização do experimento A/B foi feita por `customer_id`.

# COMMAND ----------

# Leitura do dataset de pedidos por streaming
# O arquivo é extenso, então a leitura é feita linha a linha para evitar carregar todo o volume em memória.

orders_url = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz"

user_agg = defaultdict(lambda: {
    "total_orders": 0,
    "total_revenue": 0.0,
    "last_order_date": None,
    "platforms": set(),
    "merchant_count": set()
})

response = requests.get(orders_url, stream=True)
response.raise_for_status()

with gzip.GzipFile(fileobj=response.raw) as gz:
    for i, line in enumerate(gz):
        row = json.loads(line)

        customer_id = row.get("customer_id")
        order_total_amount = row.get("order_total_amount", 0)
        order_created_at = row.get("order_created_at")
        origin_platform = row.get("origin_platform")
        merchant_id = row.get("merchant_id")

        if customer_id is None:
            continue

        user_agg[customer_id]["total_orders"] += 1
        user_agg[customer_id]["total_revenue"] += float(order_total_amount or 0)

        if order_created_at is not None:
            if (
                user_agg[customer_id]["last_order_date"] is None
                or order_created_at > user_agg[customer_id]["last_order_date"]
            ):
                user_agg[customer_id]["last_order_date"] = order_created_at

        if origin_platform is not None:
            user_agg[customer_id]["platforms"].add(origin_platform)

        if merchant_id is not None:
            user_agg[customer_id]["merchant_count"].add(merchant_id)

        if (i + 1) % 500000 == 0:
            print(f"{i+1} pedidos processados")

# COMMAND ----------

# Criação da base analítica no nível de usuário
user_metrics = pd.DataFrame([
    {
        "customer_id": customer_id,
        "total_orders": values["total_orders"],
        "total_revenue": values["total_revenue"],
        "avg_ticket": values["total_revenue"] / values["total_orders"] if values["total_orders"] > 0 else 0,
        "last_order_date": values["last_order_date"],
        "n_platforms": len(values["platforms"]),
        "platforms_used": ", ".join(sorted(values["platforms"])),  # plataformas utilizadas
        "n_merchants": len(values["merchant_count"])
    }
    for customer_id, values in user_agg.items()
])

# Enriquecimento da base com grupo do experimento
user_metrics = user_metrics.merge(
    ab_test[["customer_id", "is_target"]],
    on="customer_id",
    how="left"
)

# Enriquecimento com atributos do consumidor (incluindo nome)
user_metrics = user_metrics.merge(
    consumers[["customer_id", "customer_name", "created_at", "active", "language"]],
    on="customer_id",
    how="left"
)

# Tratamento de nomes nulos
user_metrics["customer_name"] = user_metrics["customer_name"].fillna("Unknown")

# Validação da base final
total_users = user_metrics.shape[0]
target_users = user_metrics[user_metrics["is_target"] == "target"].shape[0]
control_users = user_metrics[user_metrics["is_target"] == "control"].shape[0]

print("Validação da base analítica\n")
print(f"Total de usuários na base: {total_users:,}".replace(",", "."))
print(f"Usuários Target: {target_users:,}".replace(",", "."))
print(f"Usuários Control: {control_users:,}".replace(",", "."))

print("\nPreview da base:")

preview_df = user_metrics[[
    "customer_name",
    "customer_id",
    "total_orders",
    "total_revenue",
    "avg_ticket",
    "last_order_date",
    "platforms_used",
    "n_platforms",
    "n_merchants",
    "is_target"
]].head(10).copy()

# Arredondar valores numéricos
preview_df["total_revenue"] = preview_df["total_revenue"].round(2)
preview_df["avg_ticket"] = preview_df["avg_ticket"].round(2)

# Formatar moeda no padrão brasileiro
preview_df["total_revenue"] = preview_df["total_revenue"].apply(
    lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)
preview_df["avg_ticket"] = preview_df["avg_ticket"].apply(
    lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)

preview_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seleção de variáveis e uso dos datasets
# MAGIC
# MAGIC Nem todos os campos disponíveis nos datasets foram utilizados.
# MAGIC
# MAGIC Foram selecionadas as variáveis com maior aderência às hipóteses de negócio e às métricas necessárias para avaliar o experimento A/B, priorizando eficiência, clareza analítica e foco nas perguntas centrais do case.
# MAGIC
# MAGIC O dataset de restaurantes foi explorado para entendimento do contexto de oferta, mas não foi utilizado como eixo central da análise, já que o objetivo principal é medir o impacto da campanha de cupons sobre o comportamento dos usuários.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indicadores de sucesso da campanha
# MAGIC
# MAGIC Para mensurar o sucesso da campanha, foram definidos os seguintes indicadores:
# MAGIC
# MAGIC - **Pedidos médios por usuário**: mede se o cupom aumentou a frequência de compra.
# MAGIC - **Receita média por usuário**: mede o impacto econômico direto da campanha.
# MAGIC - **Ticket médio**: avalia se o efeito ocorreu apenas por aumento de volume ou também por maior valor por pedido.
# MAGIC - **Lift de pedidos e receita**: mostra o incremento relativo entre Target e Control.
# MAGIC - **Teste estatístico (t-test)**: verifica se as diferenças observadas são estatisticamente significativas.

# COMMAND ----------

ab_summary = user_metrics.groupby("is_target").agg(
    users=("customer_id", "nunique"),
    avg_orders_per_user=("total_orders", "mean"),
    avg_revenue_per_user=("total_revenue", "mean"),
    avg_ticket=("avg_ticket", "mean")
).reset_index()

ab_summary["avg_orders_per_user"] = ab_summary["avg_orders_per_user"].round(2)
ab_summary["avg_revenue_per_user"] = ab_summary["avg_revenue_per_user"].round(2)
ab_summary["avg_ticket"] = ab_summary["avg_ticket"].round(2)

print("Resumo do experimento A/B\n")

for _, row in ab_summary.iterrows():
    
    grupo = row["is_target"].capitalize()
    users = f'{int(row["users"]):,}'.replace(",", ".")
    
    print(f"{grupo}")
    print(f"  Usuários: {users}")
    print(f"  Pedidos médios por usuário: {row['avg_orders_per_user']:.2f}")
    print(f"  Receita média por usuário: R$ {row['avg_revenue_per_user']:.2f}")
    print(f"  Ticket médio: R$ {row['avg_ticket']:.2f}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise do experimento A/B
# MAGIC
# MAGIC A comparação entre os grupos **Target** e **Control** permite avaliar se a campanha de cupons gerou mudanças relevantes no comportamento dos usuários.
# MAGIC
# MAGIC As principais métricas comparadas foram:
# MAGIC
# MAGIC - número médio de pedidos por usuário
# MAGIC - receita média por usuário
# MAGIC - ticket médio
# MAGIC
# MAGIC Essas métricas indicam se os usuários expostos ao cupom apresentaram maior engajamento ou maior valor transacionado.
# MAGIC
# MAGIC Para validar se as diferenças observadas são estatisticamente significativas, foi aplicado um **teste t de duas amostras independentes** para pedidos e receita.
# MAGIC
# MAGIC Critério adotado:
# MAGIC
# MAGIC - **p-value < 0.05** → diferença estatisticamente significativa  
# MAGIC - **p-value ≥ 0.05** → diferença pode ter ocorrido por variação aleatória

# COMMAND ----------

# Teste estatístico - pedidos por usuário
target_orders = user_metrics[user_metrics["is_target"] == "target"]["total_orders"].dropna()
control_orders = user_metrics[user_metrics["is_target"] == "control"]["total_orders"].dropna()

ttest_orders = ttest_ind(target_orders, control_orders, equal_var=False, nan_policy="omit")

# Teste estatístico - receita por usuário
target_rev = user_metrics[user_metrics["is_target"] == "target"]["total_revenue"].dropna()
control_rev = user_metrics[user_metrics["is_target"] == "control"]["total_revenue"].dropna()

ttest_revenue = ttest_ind(target_rev, control_rev, equal_var=False, nan_policy="omit")

print("Resultado do teste estatístico\n")

print(f"Pedidos por usuário:")
print(f"  Estatística t: {ttest_orders.statistic:.2f}")
print(f"  p-value: {ttest_orders.pvalue:.6f}")

print("\nReceita por usuário:")
print(f"  Estatística t: {ttest_revenue.statistic:.2f}")
print(f"  p-value: {ttest_revenue.pvalue:.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Viabilidade financeira da campanha
# MAGIC
# MAGIC Além de avaliar impacto em comportamento, é necessário entender se a campanha é financeiramente sustentável.
# MAGIC
# MAGIC Como o case não informa o valor real do cupom nem sua taxa de utilização, foram adotadas as seguintes premissas:
# MAGIC
# MAGIC - valor médio do cupom: **R$ 10**
# MAGIC - taxa estimada de utilização: **30%**
# MAGIC
# MAGIC A análise financeira compara:
# MAGIC
# MAGIC - **receita incremental gerada pelos usuários do grupo Target**
# MAGIC - **custo total estimado dos cupons distribuídos**
# MAGIC
# MAGIC Essa comparação permite estimar se a campanha gera retorno positivo ou se o custo promocional supera o ganho incremental.

# COMMAND ----------

avg_target_rev = user_metrics[user_metrics["is_target"] == "target"]["total_revenue"].mean()
avg_control_rev = user_metrics[user_metrics["is_target"] == "control"]["total_revenue"].mean()

incremental_revenue_per_user = avg_target_rev - avg_control_rev
target_users = user_metrics[user_metrics["is_target"] == "target"]["customer_id"].nunique()

coupon_cost = 10
coupon_usage_rate = 0.30

estimated_coupon_cost = target_users * coupon_usage_rate * coupon_cost
estimated_incremental_revenue = target_users * incremental_revenue_per_user
estimated_roi = estimated_incremental_revenue - estimated_coupon_cost

lift_orders = (
    (float(ab_summary.loc[ab_summary["is_target"]=="target","avg_orders_per_user"].values[0]) -
     float(ab_summary.loc[ab_summary["is_target"]=="control","avg_orders_per_user"].values[0]))
    /
    float(ab_summary.loc[ab_summary["is_target"]=="control","avg_orders_per_user"].values[0])
) * 100

lift_revenue = (
    (float(ab_summary.loc[ab_summary["is_target"]=="target","avg_revenue_per_user"].values[0]) -
     float(ab_summary.loc[ab_summary["is_target"]=="control","avg_revenue_per_user"].values[0]))
    /
    float(ab_summary.loc[ab_summary["is_target"]=="control","avg_revenue_per_user"].values[0])
) * 100

print(f"Incremental revenue por usuário: R$ {incremental_revenue_per_user:,.2f}")
print(f"Usuários target: {target_users:,}".replace(",", "."))
print(f"Custo estimado cupons: R$ {estimated_coupon_cost:,.2f}")
print(f"Receita incremental estimada: R$ {estimated_incremental_revenue:,.2f}")
print(f"ROI estimado: R$ {estimated_roi:,.2f}")
print(f"Lift em pedidos (%): {lift_orders:.2f}")
print(f"Lift em receita (%): {lift_revenue:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segmentação de usuários
# MAGIC
# MAGIC Para entender quais perfis de usuários respondem melhor ao incentivo de cupons, os usuários foram segmentados com base na **frequência histórica de pedidos**.
# MAGIC
# MAGIC A frequência é um dos principais indicadores de engajamento em marketplaces e permite identificar diferentes perfis de comportamento.
# MAGIC
# MAGIC Segmentos definidos:
# MAGIC
# MAGIC - **one_order**: usuários com apenas um pedido
# MAGIC - **low_freq**: baixa recorrência
# MAGIC - **mid_freq**: recorrência intermediária
# MAGIC - **high_freq**: alta recorrência
# MAGIC
# MAGIC Esse critério foi escolhido por ser simples, interpretável e diretamente acionável para estratégias.

# COMMAND ----------

# Segmentação de usuários por frequência de pedidos
user_metrics["segment"] = pd.cut(
    user_metrics["total_orders"],
    bins=[0, 1, 3, 10, 1000],
    labels=["one_order", "low_freq", "mid_freq", "high_freq"]
)

segment_summary = user_metrics.groupby(["segment", "is_target"], observed=False).agg(
    users=("customer_id", "nunique"),
    avg_orders=("total_orders", "mean"),
    avg_revenue=("total_revenue", "mean"),
    avg_ticket=("avg_ticket", "mean")
).reset_index()

# Arredondar valores
segment_summary["avg_orders"] = segment_summary["avg_orders"].round(2)
segment_summary["avg_revenue"] = segment_summary["avg_revenue"].round(2)
segment_summary["avg_ticket"] = segment_summary["avg_ticket"].round(2)

print("Resumo por segmento de usuários\n")

for _, row in segment_summary.iterrows():

    segmento = row["segment"]
    grupo = row["is_target"].capitalize()
    users = f'{int(row["users"]):,}'.replace(",", ".")

    print(f"Segmento: {segmento} | Grupo: {grupo}")
    print(f"  Usuários: {users}")
    print(f"  Pedidos médios: {row['avg_orders']:.2f}")
    print(f"  Receita média por usuário: R$ {row['avg_revenue']:.2f}")
    print(f"  Ticket médio: R$ {row['avg_ticket']:.2f}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recomendações e próximos passos
# MAGIC
# MAGIC Com base na análise realizada, foram identificadas oportunidades para aumentar a eficiência da estratégia de cupons e melhorar o retorno das campanhas promocionais.
# MAGIC
# MAGIC ### Principais aprendizados
# MAGIC
# MAGIC A comparação entre os grupos **Target** e **Control** permite avaliar se a campanha de cupons gerou mudanças relevantes no comportamento dos usuários.
# MAGIC
# MAGIC Os resultados indicam que a campanha apresentou impacto nas métricas de pedidos e receita entre os grupos analisados. A análise estatística permite avaliar se essas diferenças são significativas e a análise financeira indica se o retorno gerado compensa o custo promocional da iniciativa.
# MAGIC
# MAGIC Além disso, a segmentação dos usuários mostrou que diferentes perfis de comportamento respondem de forma distinta aos incentivos promocionais.
# MAGIC
# MAGIC ### Ações sugeridas por segmento
# MAGIC
# MAGIC Com base na segmentação por frequência de pedidos, é possível direcionar estratégias específicas para cada perfil de usuário:
# MAGIC
# MAGIC - **one_order**  
# MAGIC   foco em ativação e incentivo à segunda compra, utilizando campanhas de onboarding ou cupons de primeira recorrência.
# MAGIC
# MAGIC - **low_freq**  
# MAGIC   principal público para campanhas de cupons, pois apresentam maior potencial incremental de pedidos quando expostos a incentivos.
# MAGIC
# MAGIC - **mid_freq**  
# MAGIC   testar incentivos moderados ou campanhas sazonais para estimular maior frequência de compra.
# MAGIC
# MAGIC - **high_freq**  
# MAGIC   reduzir subsídios promocionais e priorizar estratégias de retenção e personalização, já que esses usuários possuem alta propensão natural à compra.
# MAGIC
# MAGIC ### Estratégias recomendadas
# MAGIC
# MAGIC A análise sugere que campanhas de cupons tendem a ser mais eficientes quando aplicadas de forma **segmentada**, priorizando usuários com menor frequência de pedidos.
# MAGIC
# MAGIC Assim, recomenda-se:
# MAGIC
# MAGIC - direcionar cupons principalmente para usuários de **baixa frequência**
# MAGIC - evitar subsídios amplos para usuários **altamente recorrentes**
# MAGIC - utilizar segmentação comportamental para otimizar campanhas promocionais
# MAGIC - monitorar indicadores de retenção para avaliar impacto no médio prazo
# MAGIC
# MAGIC ### Novo experimento A/B sugerido
# MAGIC
# MAGIC Para evoluir a estratégia de cupons e otimizar o retorno das campanhas, recomenda-se executar um novo experimento A/B com diferentes níveis de incentivo.
# MAGIC
# MAGIC Proposta de desenho experimental:
# MAGIC
# MAGIC - **Grupo A:** sem cupom (controle)
# MAGIC - **Grupo B:** cupom padrão
# MAGIC - **Grupo C:** cupom mais agressivo para usuários de baixa frequência
# MAGIC
# MAGIC ### Métricas de avaliação
# MAGIC
# MAGIC O novo experimento deve acompanhar indicadores como:
# MAGIC
# MAGIC - pedidos por usuário
# MAGIC - receita incremental por usuário
# MAGIC - ROI da campanha
# MAGIC - retenção após a campanha
# MAGIC
# MAGIC Essas métricas permitem avaliar não apenas o impacto imediato da promoção, mas também o efeito no comportamento dos usuários ao longo do tempo.
# MAGIC
# MAGIC ### Próximos passos
# MAGIC
# MAGIC 1. executar novos testes segmentados com diferentes incentivos promocionais  
# MAGIC 2. avaliar impacto de diferentes valores de cupom sobre o comportamento de compra  
# MAGIC 3. acompanhar métricas de retenção e frequência de pedidos após as campanhas  
# MAGIC
# MAGIC Ao concentrar incentivos em segmentos com maior potencial incremental, espera-se aumentar a eficiência do investimento promocional, reduzir subsídios pouco eficientes e melhorar o **retorno sobre investimento (ROI)** das campanhas futuras.