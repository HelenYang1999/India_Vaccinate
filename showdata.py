from pyecharts import options as opts
from pyecharts.charts import Bar, Geo, Map
from pyecharts.charts import Line
from pyecharts.charts import WordCloud
from pyecharts.charts import Pie
from pyecharts.charts import Funnel
from pyecharts.charts import PictorialBar
from pyecharts.globals import SymbolType, ThemeType, ChartType
import json
from pyecharts.faker import Faker

#1.统计印度每日接种的疫苗的人数
#柱状图
def draw_chart1(result1):
    bar = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.WESTEROS))
             .add_xaxis(result1['updated_on'].values.tolist())
             # .add_yaxis("每日接种人数", result1['total_individuals_vaccinated'].values, is_stack=True)
             .add_yaxis("第一次接种人数", result1['first_dose'].values.tolist(), stack="stack1")
             .add_yaxis("第二次接种人数", result1['second_dose'].values.tolist(), stack="stack1")
             .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
             .set_global_opts(title_opts=opts.TitleOpts(title="印度每日接种疫苗人数"))

    )
    bar.render("result/result1.html")
    #渲染成图片
    # make_snapshot(snapshot, bar.render(), "bar.png")


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
                              # toolbox_opts=opts.ToolboxOpts(is_show=True),
                              xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False))
        )
    line.render("result/result21.html")
    #渲染成图片
    # make_snapshot(snapshot, bar.render(), "bar.png")

#词云图
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

#漏斗图
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
            # tooltip_opts=opts.TooltipOpts(
            #     trigger="item", formatter="{b}<br/>{c} (p / km2)"
            # ),
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


if __name__ == "__main__":
    draw_Map()