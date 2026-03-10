# Análise de Experimento A/B – Campanha de Cupons
Projeto desenvolvido como parte do processo seletivo para a posição de **Data Analyst no iFood**.

## Visão Geral

Este projeto apresenta a análise de um experimento A/B realizado pelo iFood para avaliar o impacto de uma campanha de cupons.

Os usuários foram divididos em dois grupos:

- **Control**: usuários que não receberam cupons
- **Target**: usuários que receberam cupons

O objetivo da análise é entender se a campanha gerou impacto real no comportamento dos usuários e se a iniciativa é financeiramente sustentável para o negócio.

---

# Objetivos da Análise

A análise busca responder às seguintes perguntas:

1. A campanha de cupons aumentou o número de pedidos e a receita?
2. A diferença observada entre os grupos é estatisticamente significativa?
3. A campanha é financeiramente viável considerando o custo dos cupons?
4. Quais segmentos de usuários respondem melhor à estratégia de cupons?
5. Quais melhorias podem ser aplicadas em futuras campanhas?

---

# Conjuntos de Dados Utilizados

Os dados utilizados na análise foram disponibilizados no próprio desafio e acessados diretamente via URL.

| Dataset | Descrição |
|-------|-------|
| **orders** | dataset transacional contendo todos os pedidos |
| **consumers** | informações de perfil dos usuários |
| **restaurants** | informações sobre os restaurantes |
| **ab_test_ref** | identificação dos usuários pertencentes ao grupo Target |

O experimento foi randomizado no nível de **usuário (`customer_id`)**.

---

# Processamento e Preparação dos Dados

O dataset de pedidos contém milhões de registros.  
Para processar esses dados de forma eficiente, foi utilizada uma estratégia de **leitura por streaming**, na qual os registros são processados linha a linha sem carregar todo o arquivo na memória.

Durante esse processo foram agregadas métricas por usuário, incluindo:

- número total de pedidos
- receita total gerada
- ticket médio
- número de plataformas utilizadas
- número de restaurantes distintos

Após a criação dessas métricas, os dados foram enriquecidos com:

- grupo do experimento A/B
- informações de perfil dos usuários

Isso permitiu construir uma **base analítica no nível de usuário** para análise do experimento.

---

# Indicadores de Sucesso da Campanha

Para avaliar o desempenho da campanha, foram utilizados os seguintes indicadores:

- **Pedidos médios por usuário**
- **Receita média por usuário**
- **Ticket médio**
- **Lift entre os grupos Target e Control**
- **Teste estatístico (t-test)** para validar significância das diferenças observadas

Esses indicadores permitem avaliar tanto o impacto em comportamento quanto o impacto econômico da campanha.

---

# Análise do Experimento A/B

A comparação entre os grupos **Target** e **Control** foi realizada para identificar possíveis diferenças em:

- frequência de pedidos
- receita média por usuário
- valor médio por pedido

Para verificar se as diferenças observadas são estatisticamente significativas, foi aplicado um **teste t de duas amostras independentes**.

Critério utilizado:

- **p-value < 0.05** → diferença estatisticamente significativa
- **p-value ≥ 0.05** → diferença pode ser resultado de variação aleatória

---

# Avaliação de Viabilidade Financeira

Além da análise comportamental, também foi realizada uma estimativa de viabilidade financeira da campanha.

Como o case não informa o valor real do cupom nem a taxa de utilização, foram adotadas as seguintes premissas:

- valor médio do cupom: **R$ 10**
- taxa estimada de utilização: **30%**

A análise compara:

- receita incremental gerada pelos usuários do grupo Target
- custo total estimado dos cupons distribuídos

Isso permite avaliar se a campanha gera retorno positivo para o negócio.

---

# Segmentação de Usuários

Para entender quais perfis respondem melhor à campanha, os usuários foram segmentados com base na frequência histórica de pedidos.

Segmentos definidos:

| Segmento | Descrição |
|-------|-------|
| one_order | usuários com apenas um pedido |
| low_freq | usuários com baixa frequência de pedidos |
| mid_freq | usuários com frequência intermediária |
| high_freq | usuários com alta frequência de pedidos |

Essa segmentação permite identificar onde a campanha gera maior impacto incremental.

---

# Principais Insights

A análise indica que o impacto da campanha pode variar entre diferentes segmentos de usuários.

Usuários com **menor frequência de pedidos** tendem a apresentar maior potencial de incremento quando expostos a incentivos promocionais.

Já usuários altamente recorrentes possuem maior propensão natural à compra, o que pode reduzir a eficiência de campanhas baseadas exclusivamente em cupons.

---

# Recomendações Estratégicas

Com base na análise realizada, são recomendadas as seguintes ações:

- priorizar campanhas de cupons para **usuários de baixa frequência**
- evitar subsídios amplos para usuários altamente recorrentes
- realizar novos **experimentos A/B segmentados**
- testar diferentes valores de cupom para medir elasticidade de resposta

---

# Proposta de Novo Experimento A/B

Para evoluir a estratégia de cupons, recomenda-se testar:

- **Grupo A:** sem cupom
- **Grupo B:** cupom padrão
- **Grupo C:** cupom mais agressivo para usuários de baixa frequência

Métricas de avaliação:

- pedidos por usuário
- receita incremental
- ROI por segmento
- retenção após campanha

---

# Estrutura do Projeto

# Tecnologias Utilizadas
As seguintes tecnologias foram utilizadas no desenvolvimento da análise:

- Python
- Pandas
- NumPy
- SciPy
- Databricks

Essas ferramentas foram utilizadas para ingestão de dados, processamento, análise estatística e visualização dos resultados.

---

# Como Executar o Projeto
Para reproduzir a análise:

1. Abra o notebook `case_ifood - data analysis.py` em um ambiente Python ou no Databricks.
2. Execute as células do notebook sequencialmente.
3. Os datasets são carregados automaticamente a partir das URLs disponibilizadas no desafio.

# Autor
Nathan Alves  
