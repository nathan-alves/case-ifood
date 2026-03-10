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

# Leitura dos datasets auxiliares

consumers = pd.read_csv(
    "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
    compression="gzip"
)

restaurants = pd.read_csv(
    "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
    compression="gzip"
)

# O arquivo do experimento está compactado em formato tar.gz.
# Por isso, o CSV interno é extraído programaticamente antes da leitura.
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
# MAGIC Os dados foram acessados programaticamente a partir das URLs públicas disponibilizadas no case.
# MAGIC
# MAGIC O arquivo de pedidos foi processado por **streaming**, linha a linha, para evitar carregar milhões de registros em memória ao mesmo tempo.
# MAGIC
# MAGIC A base analítica foi construída no nível de **usuário**, pois a randomização do experimento A/B foi feita por `customer_id`.

# COMMAND ----------

# Leitura do dataset de pedidos por streaming
# O arquivo possui milhões de registros, então a leitura é feita linha a linha
# para evitar carregar todo o volume em memória.

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
        "n_merchants": len(values["merchant_count"])
    }
    for customer_id, values in user_agg.items()
])

# Enriquecimento da base com grupo do experimento e atributos de usuários
user_metrics = user_metrics.merge(
    ab_test[["customer_id", "is_target"]],
    on="customer_id",
    how="left"
)

user_metrics = user_metrics.merge(
    consumers[["customer_id", "created_at", "active", "language"]],
    on="customer_id",
    how="left"
)

# Validação da base final
print(user_metrics.shape)
print(user_metrics["is_target"].value_counts(dropna=False))
user_metrics.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seleção de variáveis e uso dos datasets
# MAGIC
# MAGIC Nem todos os campos disponíveis nos datasets foram utilizados.
# MAGIC
# MAGIC Foram selecionadas as variáveis com maior aderência às hipóteses de negócio e às métricas necessárias para avaliar o experimento A/B, priorizando eficiência computacional, clareza analítica e foco nas perguntas centrais do case.
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
ab_summary["users"] = ab_summary["users"].apply(lambda x: f"{x:,}".replace(",", "."))

ab_summary

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

print("Teste estatístico - pedidos:", ttest_orders)
print("Teste estatístico - receita:", ttest_revenue)

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
# MAGIC Esse critério foi escolhido por ser simples, interpretável e diretamente acionável para estratégias de CRM e growth.

# COMMAND ----------

user_metrics["segment"] = pd.cut(
    user_metrics["total_orders"],
    bins=[0, 1, 3, 10, 1000],
    labels=["one_order", "low_freq", "mid_freq", "high_freq"]
)

segment_summary = user_metrics.groupby(["segment", "is_target"]).agg(
    users=("customer_id", "nunique"),
    avg_orders=("total_orders", "mean"),
    avg_revenue=("total_revenue", "mean"),
    avg_ticket=("avg_ticket", "mean")
).reset_index()

segment_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recomendações e próximos passos
# MAGIC
# MAGIC Com base na análise realizada, algumas oportunidades de melhoria podem aumentar a eficiência da estratégia de cupons.
# MAGIC
# MAGIC ### Principais aprendizados
# MAGIC
# MAGIC A campanha apresentou impacto nas métricas de pedidos e receita entre os grupos analisados. A análise estatística permite avaliar se essas diferenças são significativas e a análise financeira indica se o retorno gerado compensa o custo promocional.
# MAGIC
# MAGIC ### Estratégias recomendadas
# MAGIC
# MAGIC - direcionar cupons principalmente para usuários de **baixa frequência**, onde o potencial incremental tende a ser maior
# MAGIC - evitar oferecer cupons amplamente para usuários **altamente recorrentes**, que já possuem alta propensão de compra
# MAGIC - utilizar segmentação comportamental para otimizar campanhas promocionais
# MAGIC
# MAGIC ### Novo experimento A/B sugerido
# MAGIC
# MAGIC Para melhorar a estratégia de cupons, recomenda-se um novo teste A/B:
# MAGIC
# MAGIC - **Grupo A:** sem cupom
# MAGIC - **Grupo B:** cupom padrão
# MAGIC - **Grupo C:** cupom mais agressivo para usuários de baixa frequência
# MAGIC
# MAGIC ### Métricas de avaliação
# MAGIC
# MAGIC - pedidos por usuário
# MAGIC - receita incremental por usuário
# MAGIC - ROI da campanha
# MAGIC - retenção após a campanha
# MAGIC
# MAGIC ### Próximos passos
# MAGIC
# MAGIC 1. executar novos testes segmentados
# MAGIC 2. avaliar impacto de diferentes valores de cupom
# MAGIC 3. acompanhar retenção de médio prazo
# MAGIC
# MAGIC Essa abordagem pode aumentar a eficiência do investimento promocional e melhorar o retorno sobre campanhas futuras.

# COMMAND ----------

# MAGIC %md
# MAGIC 1️⃣ “Como você garante que esse A/B test é confiável?”
# MAGIC O que o gestor quer avaliar
# MAGIC
# MAGIC Se você entende validade experimental.
# MAGIC
# MAGIC Resposta clara
# MAGIC
# MAGIC Você pode responder algo assim:
# MAGIC
# MAGIC Primeiro eu verificaria se os grupos Target e Control foram corretamente randomizados e possuem tamanhos similares.
# MAGIC Depois avaliaria possíveis vieses de seleção ou diferenças estruturais entre os grupos antes do experimento.
# MAGIC
# MAGIC Além disso, aplicamos um teste estatístico para avaliar se as diferenças observadas são significativas e não resultado de variabilidade aleatória.
# MAGIC
# MAGIC Se quiser enriquecer:
# MAGIC
# MAGIC Também é importante verificar se houve contaminação entre grupos ou mudanças externas durante o experimento que possam ter afetado os resultados.
# MAGIC
# MAGIC 2️⃣ “Se o teste mostrar resultado positivo, o que você faria antes de escalar a campanha?”
# MAGIC O que querem avaliar
# MAGIC
# MAGIC Pensamento de produto e rollout.
# MAGIC
# MAGIC Boa resposta
# MAGIC
# MAGIC Antes de escalar para toda a base eu faria um rollout gradual, por exemplo expandindo para uma porcentagem maior de usuários.
# MAGIC
# MAGIC Também avaliaria o impacto por segmento de usuário para entender onde o incentivo gera maior retorno e evitar oferecer cupons onde não há impacto incremental.
# MAGIC
# MAGIC Você também pode citar:
# MAGIC
# MAGIC monitorar ROI
# MAGIC
# MAGIC acompanhar métricas de retenção
# MAGIC
# MAGIC 3️⃣ “Quais limitações você vê na sua análise?”
# MAGIC
# MAGIC Essa é uma das perguntas mais comuns.
# MAGIC
# MAGIC Boa resposta
# MAGIC
# MAGIC Algumas limitações da análise são:
# MAGIC
# MAGIC Não avaliamos comportamento pré-experimento dos usuários, o que poderia ajudar a controlar melhor diferenças estruturais.
# MAGIC
# MAGIC A análise considera apenas pedidos e receita no período observado, sem avaliar impacto de longo prazo na retenção.
# MAGIC
# MAGIC As premissas financeiras utilizadas para o ROI são simplificadas, como taxa de uso do cupom e custo médio.
# MAGIC
# MAGIC Isso demonstra pensamento crítico, algo muito valorizado.
# MAGIC
# MAGIC 4️⃣ “Se você pudesse melhorar esse experimento, o que faria?”
# MAGIC
# MAGIC Aqui querem ver capacidade de evoluir o produto.
# MAGIC
# MAGIC Boa resposta
# MAGIC
# MAGIC Eu proporia novos testes segmentados por comportamento do usuário.
# MAGIC
# MAGIC Por exemplo:
# MAGIC
# MAGIC usuários de baixa frequência
# MAGIC
# MAGIC usuários recém adquiridos
# MAGIC
# MAGIC usuários com risco de churn
# MAGIC
# MAGIC Dessa forma conseguimos direcionar incentivos onde o impacto incremental tende a ser maior.
# MAGIC
# MAGIC Outra boa ideia:
# MAGIC
# MAGIC Também testaria diferentes valores de cupom para entender a elasticidade da resposta.
# MAGIC
# MAGIC 5️⃣ “Qual insight mais importante você tirou dessa análise?”
# MAGIC
# MAGIC Essa pergunta avalia capacidade de síntese.
# MAGIC
# MAGIC Boa resposta
# MAGIC
# MAGIC O principal insight é que a campanha pode gerar aumento de pedidos e receita, mas esse impacto não é uniforme entre os usuários.
# MAGIC
# MAGIC A análise de segmentação mostra que determinados perfis respondem mais ao incentivo, indicando que uma estratégia de cupons mais direcionada pode melhorar significativamente o retorno da campanha.
# MAGIC
# MAGIC Se quiser deixar ainda mais forte:
# MAGIC
# MAGIC Isso sugere que personalização de incentivos pode ser mais eficiente do que campanhas amplas.