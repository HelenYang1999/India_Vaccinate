# 2021年印度新冠肺炎疫苗接种数据分析

作者：厦门大学信息学院计算机科学系2020级研究生 杨琪
指导老师：厦门大学数据库实验室 林子雨 博士/副教授

本案例以2021年印度新冠肺炎疫苗接种数据作为数据集，以Python为编程语言，使用Flink对数据进行分析，并对分析结果进行可视化。

## 一、实验环境

## 1、环境列表

（1）Windows：10 (x64) 
（2）Anaconda：4.10.1
（3）Python: 3.7
（4）Flink: 1.13.1 
（5）Pycharm：2020.2.3

## 2、环境安装

在Anaconda中创建一个虚拟环境取名为pyflink

```shell
conda create -n pyflink python=3.7
```

安装flink

```shell
python -m pip install apache-flink
```

这里踩了一个小坑，一开始安装不成功，报错ValueError: check_hostname requires server_hostname。解决办法是关闭代理，就成功安装。

## 二、数据集

### 1. 数据集下载

本次作业使用的数据集来自数据网站Kaggle的印度新冠肺炎疫情数据集（COVID-19 in India），该数据集有三个文件，我选取了covid_vaccine_statewise.csv，其与疫苗接种相关的数据表进行分析，其中包含了印度从2021-01-16至我下载当日2021-05-31的相关数据。如下图所示，数据包含以下字段：

![1](C:\Users\lin\Desktop\1.png)



| 表格字段                            | 对应意思                 | 示例               |
| ----------------------------------- | ------------------------ | ------------------ |
| Updated On                          | 日期                     | 16/01/2021         |
| State                               | 州（包括国家）           | India；Maharashtra |
| Total Individuals Vaccinated        | 接种总人数               | 99449              |
| Total Sessions Conducted            | 进行的总会话             | 13611              |
| Total Sites                         | 总站点数                 | 6583               |
| First Dose Administered             | 第一次接种人数           | 99449              |
| Second Dose Administered            | 第二次接种人数           | 0                  |
| Male(Individuals Vaccinated)        | 男性（接种疫苗的个体）   | 41361              |
| Female(Individuals Vaccinated)      | 女性（接种疫苗的个体）   | 58083              |
| Transgender(Individuals Vaccinated) | 变性人（接种疫苗的个体） | 5                  |
| Total Covaxin Administered          | Covaxin总疫苗数          | 1299               |
| Total CoviShield Administered       | CoviShield总疫苗数       | 98150              |
| Total Sputnik V Administered        | Sputnik V总疫苗数        | 0                  |
| AEFI                                | AEFI疑似预防接种异常反应 | 381                |
| 18-45 years (Age)                   | 18-45岁（年龄）          | 32654              |
| 45-60 years (Age)                   | 45-60岁（年龄）          | 33016              |
| 60+ years (Age)                     | 60+岁（年龄）            | 33779              |
| Total Doses Administered            | 所有接种过的人次         | 99449              |

其中，AEFI和年龄段数据是从2021-03-16开始统计的。

### 2.格式转换

#### 2.1日期格式转换

原本日期的存储格式是07/03/2021，但为了后续的比较操作，我将日期格式进行了转换，将07/03/2021转换成2021-03-07的格式。

```python
def translate_date(old_date):
    newdate = old_date[6:10] + '-' +old_date[3:5] + '-' + old_date[0:2]
    return newdate
```

#### 2.2 csv数据处理

因为AEFI 和年龄段的数据是从2021-03-16之后才开始统计的，所以读取之后显示的都是NaN值，所以使用pandas操作将NaN值全部替换成了0。

```python
# 将所有的NaN转换成0
pdf = pdf.fillna(0)
```

## 三、使用Flink对数据进行分析

Flink框架支持三种语言：Java、scala、python。因为作者对python最为熟悉，所以这里采用python作为编程语言，本部分操作的完整代码放在了main.py中，具体如下。

### 1、读取文件生成PyFlink Table

先使用pandas库读取csv操作，并做刚刚提过的两个数据转换操作。

```python
pdf = pd.read_csv("data/covid_vaccine_statewise.csv")
pdf['Updated On'] = pdf['Updated On'].map(lambda x: translate_date(x))
# 将所有的NaN转换成0
pdf = pdf.fillna(0)
```

将数据从csv文件读取成pandas的DataFrame后，准确flink环境

```python
# 获取执行环境
exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)

# 创建TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = StreamTableEnvironment.create(exec_env, environment_settings=env_settings)
```

另外，PyFlink支持将Pandas DataFrame转换成PyFlink表。在内部实现上，会在客户端将Pandas DataFrame序列化成Arrow列存格式，序列化后的数据在作业执行期间，在Arrow源中会被反序列化，并进行处理。

操作如下所示。

```python
# 由Pandas DataFrame创建列名和列类型的PyFlink表
input_table = table_env.from_pandas(pdf,DataTypes.ROW(
   [DataTypes.FIELD("updated_on", DataTypes.STRING()),                       DataTypes.FIELD("state",DataTypes.STRING()),
    DataTypes.FIELD("total_individuals_vaccinated", DataTypes.FLOAT()),     DataTypes.FIELD("total_sessions_conducted",DataTypes.FLOAT()),
    DataTypes.FIELD("total_sites", DataTypes.FLOAT()),  		             DataTypes.FIELD("first_dose",DataTypes.FLOAT()),
    DataTypes.FIELD("second_dose", DataTypes.FLOAT()),
    DataTypes.FIELD("male", DataTypes.FLOAT()),
    DataTypes.FIELD("female", DataTypes.FLOAT()),
    DataTypes.FIELD("transgender",DataTypes.FLOAT()),
    DataTypes.FIELD("Covaxin", DataTypes.FLOAT()),
    DataTypes.FIELD("CoviShield",DataTypes.FLOAT()),
    DataTypes.FIELD("Sputnik_V", DataTypes.FLOAT()),                         DataTypes.FIELD("AEFI", DataTypes.FLOAT()),                             DataTypes.FIELD("first_age",DataTypes.FLOAT()),
    DataTypes.FIELD("second_age", DataTypes.FLOAT()),
    DataTypes.FIELD("third_age",DataTypes.FLOAT()),
    DataTypes.FIELD("total_doses_administered", DataTypes.FLOAT())]))

```

### 2、进行数据分析

本实验主要统计以下8个指标，分别是：

1）统计印度每日接种的疫苗的人数，分为第一针和第二针。做法是选择state等于India的字段中的total_individuals_vaccinated、first_dose、second_dose列。结果转换成pandas的DataFrame结构，保存在result1中。

```python
#1、统计印度每日接种的疫苗的人数
df1=input_table.select(input_table.updated_on,input_table.state,input_table.total_individuals_vaccinated,input_table.first_dose,input_table.second_dose).where(input_table.state == "India")
result1 = df1.to_pandas()
showdata.draw_chart1(result1)
```

2）统计印度每日三种类型疫苗的使用情况。印度现拥有三种疫苗，两种是本地制造的冠状病毒疫苗：CoviShield 和 Covaxin。Sputnik V 疫苗于 4 月获准使用，现已上市。具体做法是选择state等于India的字段中的Covaxin、CoviShield、Sputnik_V列。结果转换成pandas的DataFrame结构，保存在result2中。

```python
df2=input_table.select(input_table.updated_on,input_table.state,input_table.Covaxin,input_table.CoviShield,input_table.Sputnik_V).where(input_table.state == 'India')
result2 = df2.to_pandas()
#折线图
showdata.draw_chart2_1(result2)
```

3）统计截止到5.31日，印度各个州的累积疫苗接种情况。具体做法是以updated_on和state作为分组字段，对total_individuals_vaccinated进行汇总统计。再使用alias方法对表中字段进行改名，便于后续操作。结果转换成pandas的DataFrame结构，保存在result3中。

```python
df3=input_table.group_by("updated_on,state").select(input_table.updated_on,input_table.state,input_table.total_individuals_vaccinated.sum).where(input_table.updated_on == "2021-05-31")
df3 = df3.alias("updated_on,state,sum_vaccinated")
result3 = df3.to_pandas()
#词云图
showdata.draw_chart3(result3)
#地图
showdata.draw_Map(result3)
```

4）找出印度接种疫苗最多的10个州，在第三个的结果上进行操作，先排除state等于India的字段，再对剩余的按照sum_vaccinated字段进行排序，找出前十个州。结果转换成pandas的DataFrame结构，保存在result4中。

```python
df4 = df3.select(df3.state,df3.sum_vaccinated).where(df3.state != "India").order_by(df3.sum_vaccinated.desc).limit(10)
result4 = df4.to_pandas()
showdata.draw_chart4(result4)
```

5）找出印度接种疫苗最少的10个州，操作同四，将降序改成了升序。结果转换成pandas的DataFrame结构，保存在result5中。

```python
df5=df3.select(df3.state,df3.sum_vaccinated).order_by(df3.sum_vaccinated.asc).limit(10)
result5 = df5.to_pandas()
showdata.draw_chart5(result5)
```

6）统计截止到5.31各州的男女（变性人）接种总数。具体做法是先过滤掉所有state等于India的字段，然后按照state,updated_on进行分组，对male、female、transgender字段进行汇总统计。再使用alias方法对表中字段进行改名，便于后续操作。结果转换成pandas的DataFrame结构，保存在result6中。

```python
df6 = input_table.filter(input_table.state != "India").group_by("state,updated_on").select(input_table.updated_on,input_table.state,input_table.male.sum,input_table.female.sum,input_table.transgender.sum,input_table.total_individuals_vaccinated.sum).where(input_table.updated_on == "2021-05-31")
df6 = df6.drop_columns(df6.updated_on)
df6 = df6.alias("state,sum_male,sum_female,sum_transgender,sum_total")
result6 = df6.to_pandas()
showdata.draw_chart6(result6)
```

7）统计截止到2021-05-01，印度总人口接种率，印度总人口为1,383,500,000，数据来自维基百科。具体做法是先选择所有state等于India的字段，再选择updated_on小于等于2021-05-01的字段，以updated_on字段分组，然后对first_dose字段进行汇总。结果转换成pandas的DataFrame结构，保存在result7中。

```python
df7 = input_table.filter(input_table.state == "India").filter(input_table.updated_on <= "2021-05-01").group_by(input_table.updated_on).select(input_table.updated_on,input_table.first_dose.sum).where(input_table.updated_on == "2021-05-01")
df7 = df7.alias("state,sum")
result7 = df7.to_pandas()
showdata.draw_chart7(result7)
```

8）印度各州每日的接种年龄分布比例，数据来自2021-03-16到2021-05-31时间段。具体做法选择updated_on小于等于2021-05-31且大于等于2021-03-16的字段，以updated_on和state字段分组，然后对各年龄字段进行汇总。最后计算每个年龄段的比例，保存在rate_1、rate_2、rate_3字段中。最后结果转换成pandas的DataFrame结构，保存在result8中。

```python
df8 = input_table.filter(input_table.updated_on >= "2021-03-16").group_by("state,updated_on").select(input_table.updated_on,input_table.state,input_table.first_age.sum,input_table.second_age.sum,input_table.third_age.sum,input_table.total_individuals_vaccinated.sum).where(input_table.updated_on == "2021-05-31")
df8 = df8.alias("updated_on,state,sum1,sum2,sum3,sum")
df8 = df8.add_columns((df8.sum1 / df8.sum).alias("rate_1"))
df8 = df8.add_columns((df8.sum2 / df8.sum).alias("rate_2"))
df8 = df8.add_columns((df8.sum3 / df8.sum).alias("rate_3"))
result8 = df8.to_pandas()
showdata.draw_chart8(result8)
```

## 四、数据可视化

### 1、可视化工具选择

选择使用python第三方库pyecharts作为可视化工具。
在使用前，需要安装pyecharts，安装代码如下：

```shell
pip install pyecharts
```

### 2、数据展示

本实验对分析的8个指标画了9个图进行可视化，图标类型有柱状图、折线图、饼图、漏斗图、词云图、地图、象型状图等7种图形。

1）对印度每日接种的疫苗的人数使用柱状图显示。代码如下：

```python
def draw_chart1(result1):
    bar = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.WESTEROS))
             .add_xaxis(result1['updated_on'].values.tolist())
             .add_yaxis("第一次接种人数", result1['first_dose'].values.tolist(), stack="stack1")
             .add_yaxis("第二次接种人数", result1['second_dose'].values.tolist(), stack="stack1")
             .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
             .set_global_opts(title_opts=opts.TitleOpts(title="印度每日接种疫苗人数"))

    )
    bar.render("result/result1.html")
```

可视化结果如下，可视化结果是html格式的，结果展示图保存在result文件夹下。


2）印度每日三种类型疫苗的使用情况用折线图显示，代码如下：

```python
#折线图
def draw_chart2(result2):
    line = (
        Line(init_opts=opts.InitOpts(theme=ThemeType.WESTEROS))
             .add_xaxis(result2['updated_on'].values.tolist())
             .add_yaxis("Covaxin疫苗", result2['Covaxin'].values.tolist())
             .add_yaxis("CoviShield疫苗", result2['CoviShield'].values.tolist())
             .add_yaxis("Sputnik_V疫苗", result2['Sputnik_V'].values.tolist())
             .set_series_opts(label_opts=opts.LabelOpts(is_show=True))
             .set_global_opts(title_opts=opts.TitleOpts(title="印度每日三种疫苗使用情况"),
                              tooltip_opts=opts.TooltipOpts(trigger="axis"),
                              xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False))
        )
    line.render("result/result2.html")
```

可视化结果如下。



3）对截止到5.31日印度各个州的累积疫苗接种情况采用两种形式的图显示。

①第一种是词云图，根据接种量的大小，显示对应州的名称，接种量越大，名称越显眼。代码如下。

```python
def draw_chart3(result3):
    words = []
    state = result3["state"].values.tolist()
    sum_vaccinated = result3["sum_vaccinated"].values.tolist()
    for i in range(1,len(state)):
        word = (state[i],sum_vaccinated[i])
        words.append(word)
    c = (
        WordCloud()
            .add("", words, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
            .set_global_opts(title_opts=opts.TitleOpts(title="截止到5.31日，印度各个州的累积疫苗接种情况"))
    )
    c.render("result/result3.html")
```


②第二种是地图，结合地图的形式，并以不同的颜色将接种人数画出来，更直接清晰，代码如下：

```python
#画印度地图
def draw_Map(result):
    states = result["state"].values.tolist()[1:]
    sum_vaccinateds = result["sum_vaccinated"].values.tolist()[1:]
    MAP_DATA = []
    NAME_MAP_DATA = {}
    for i in range(len(states)):
        MAP_DATA.append([states[i],sum_vaccinateds[i]])
        NAME_MAP_DATA[states[i]] = states[i]
    # init_opts = opts.InitOpts(width="1400px", height="800px")
    map = (
        Map(init_opts = opts.InitOpts(theme=ThemeType.WESTEROS,width="800px", height="600px"))
            .add(
            series_name="印度各州接种人数",
            maptype="印度",
            data_pair=MAP_DATA,
            name_map=NAME_MAP_DATA,
            is_map_symbol_show=False,
        )
            .set_global_opts(
            title_opts=opts.TitleOpts(
                title="印度各州接种人数（2021.1-2021.5）",
            ),
            visualmap_opts=opts.VisualMapOpts(
                min_=28000,
                max_=15000000,
                range_text=["High", "Low"],
                is_calculable=True,
                # range_color=["lightskyblue", "yellow", "orangered"],
            ),
        )
    )
    map.render("result/map.html")
```

可视化结果如下：


4）将印度接种人数前10的州的数据以象形状图显示，并且采用自定义图片，将图片从png格式转换成base64编码，保存在symbol.json文件中。代码如下。

```python
#象型状图，自定义符号
def draw_chart4(result4):

    states = result4["state"].values.tolist()
    vaccs = result4["sum_vaccinated"].values.tolist()

    with open("symbol.json", "r", encoding="utf-8") as f:
        symbols = json.load(f)

    c = (
        PictorialBar(init_opts=opts.InitOpts(theme=ThemeType.WESTEROS,width="1200px"))
            .add_xaxis(states)
            .add_yaxis(
            "",
            vaccs,
            label_opts=opts.LabelOpts(is_show=False),
            symbol_size=22,
            symbol_repeat="fixed",
            symbol_offset=[0, -5],
            is_symbol_clip=True,
            symbol=symbols["vacc"],
            color = "#187a2f",
        )
            .reversal_axis()
            .set_global_opts(
            title_opts=opts.TitleOpts(title="印度接种人数最多的10个州"),
            xaxis_opts=opts.AxisOpts(is_show=False),
            yaxis_opts=opts.AxisOpts(
                axistick_opts=opts.AxisTickOpts(is_show=False),
                axisline_opts=opts.AxisLineOpts(
                    linestyle_opts=opts.LineStyleOpts(opacity=0)
                ),
            ),
        )
    )
    c.render("result/result4.html")
```


5）将印度接种人数最后10个州的数据以漏斗图的形式显示，代码如下：

```python
def draw_chart5(result5):
    states = result5["state"].values.tolist()
    sum_vaccinateds = result5["sum_vaccinated"].values.tolist()
    datas = []
    for i in range(len(states)):
        data = [states[i],sum_vaccinateds[i]]
        datas.append(data)

    c = (
        Funnel()
            .add(
            "State",
            datas,
            label_opts=opts.LabelOpts(position="inside"),
        )
            .set_global_opts(title_opts=opts.TitleOpts(title=""))
    )
    c.render("result/result5.html")
```


6）将印度各州男女（变性人）的接种总数以柱状图显示，代码和结果图如下。

```python
def draw_chart6(result6):
    states = result6["state"].values.tolist()
    sum_male = result6["sum_male"].values.tolist()
    sum_female = result6["sum_female"].values.tolist()
    sum_transgender = result6["sum_transgender"].values.tolist()
    c = (
        Bar(opts.InitOpts(theme=ThemeType.MACARONS,width="1000px"))
            .add_xaxis(states)
            .add_yaxis("男性", sum_male)
            .add_yaxis("女性", sum_female)
            .add_yaxis("变性人", sum_transgender)
            .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
            .set_global_opts(
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-15)),
            title_opts=opts.TitleOpts(title="印度各州的男女（变性人）接种总数",subtitle="截止到2021.5.31"),
            # brush_opts=opts.BrushOpts(),
        )
    )
    c.render("result/result6.html")
```


7）将印度累计接种情况以饼状图显示，可以发现接种率还不到10%，代码和结果图如下。

```python
def draw_chart7(result7):
    vacc = result7["sum"].values.tolist()[0]
    sum = 1383500000
    un_vacc = sum - vacc
    x_data = ["已接种", "未接种"]
    y_data = [vacc,un_vacc]
    data_pair = [list(z) for z in zip(x_data, y_data)]
    c = (
        Pie(opts.InitOpts(theme=ThemeType.WESTEROS))
            .add(series_name="是否接种",data_pair=data_pair)
            .set_global_opts(title_opts=opts.TitleOpts(title="印度接种情况",subtitle="数据截止到2021-5-1，人口总数来源于维基百科"),
                                                       legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"),)
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c},({d}%)"))
    )
    c.render("result/result7.html")
```


8）将印度各州每日的接种年龄分布比例以柱状图显示，代码和结果图如下：

```python
def draw_chart8(result8):
    states = result8["state"].values.tolist()[1:]
    sum1 = result8["rate_1"].values.tolist()[1:]
    sum2 = result8["rate_2"].values.tolist()[1:]
    sum3 = result8["rate_3"].values.tolist()[1:]

    c = (
        Bar(opts.InitOpts(theme=ThemeType.WESTEROS, width="1200px"))
            .add_xaxis(states)
            .add_yaxis("18-45岁", sum1)
            .add_yaxis("45-60岁", sum2)
            .add_yaxis("60岁以上", sum3)
            .set_series_opts(label_opts=opts.LabelOpts(is_show=False),
                             markline_opts=opts.MarkLineOpts(
                                 data=[opts.MarkLineItem(y=0.33, name="yAxis=0.33")]
                             ),)
            .set_global_opts(
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-15)),
            title_opts=opts.TitleOpts(title="印度各州各年龄段接种比例", subtitle="从2021.3.16到2021.5.31"),
            # brush_opts=opts.BrushOpts(),
        )
    )
    c.render("result/result8.html")
```


