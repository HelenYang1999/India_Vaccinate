from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableConfig, BatchTableEnvironment, EnvironmentSettings, StreamTableEnvironment, DataTypes
import pandas as pd
from pyflink.table.expressions import col
import showdata

def translate_date(old_date):
    newdate = old_date[6:10] + '-' +old_date[3:5] + '-' + old_date[0:2]
    return newdate


if __name__ == "__main__":
    # 获取执行环境
    exec_env = StreamExecutionEnvironment.get_execution_environment()
    exec_env.set_parallelism(1)

    # 创建TableEnvironment
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    table_env = StreamTableEnvironment.create(exec_env, environment_settings=env_settings)
    # 读取csv文件转换成pandas的DataFrame
    pdf = pd.read_csv("data/covid_vaccine_statewise.csv")
    pdf['Updated On'] = pdf['Updated On'].map(lambda x: translate_date(x))
    # 将所有的NaN转换成0
    pdf = pdf.fillna(0)
    #将Pandas DataFrame转换为PyFlink表
    input_table = table_env.from_pandas(pdf,DataTypes.ROW([DataTypes.FIELD("updated_on", DataTypes.STRING()),
                                                    DataTypes.FIELD("state", DataTypes.STRING()),DataTypes.FIELD("total_individuals_vaccinated", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("total_sessions_conducted", DataTypes.FLOAT()),DataTypes.FIELD("total_sites", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("first_dose", DataTypes.FLOAT()),DataTypes.FIELD("second_dose", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("male", DataTypes.FLOAT()),DataTypes.FIELD("female", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("transgender", DataTypes.FLOAT()),DataTypes.FIELD("Covaxin", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("CoviShield", DataTypes.FLOAT()),DataTypes.FIELD("Sputnik_V", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("AEFI", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("first_age", DataTypes.FLOAT()),DataTypes.FIELD("second_age", DataTypes.FLOAT()),
                                                    DataTypes.FIELD("third_age", DataTypes.FLOAT()),DataTypes.FIELD("total_doses_administered", DataTypes.FLOAT())]))

    #1.统计印度每日接种的疫苗的人数（分为第一针和第二针，以不同颜色显示）
    df1 = input_table.select(input_table.updated_on, input_table.state,input_table.total_individuals_vaccinated,input_table.first_dose,input_table.second_dose).where(input_table.state == "India")
    result1 = df1.to_pandas()
    # print(result1)
    showdata.draw_chart1(result1)


    #2.统计印度每日三种类型疫苗的使用情况
    # df2 = input_table.select(input_table.updated_on,input_table.state,input_table.Covaxin, input_table.CoviShield,input_table.Sputnik_V).where(input_table.state == "India" & input_table.updated_on.toDate() >= "16/03/2021")
    df2 = input_table.select(input_table.updated_on,input_table.state,input_table.Covaxin, input_table.CoviShield,input_table.Sputnik_V).where(input_table.state == 'India')
    # df2 = df2.select(col("*")).where()
    result2 = df2.to_pandas()
    # print(result2)
    #柱状图,效果不好
    # showdata.draw_chart2(result2)
    #折线图
    showdata.draw_chart2(result2)

    #3.统计截止到5.31日，各个州的累积疫苗接种情况
    df3 = input_table.group_by("updated_on,state").select(input_table.updated_on,input_table.state,input_table.total_individuals_vaccinated.sum).where(input_table.updated_on == "2021-05-31")
    df3 = df3.alias("updated_on,state,sum_vaccinated")
    result3 = df3.to_pandas()
    print(result3)
    #词云图
    showdata.draw_chart3(result3)
    #地图
    showdata.draw_Map(result3)

    # table_env.create_temporary_view("df3",df3)
    #4.找出印度接种疫苗最多的10个州
    df4 = df3.select(df3.state,df3.sum_vaccinated).where(df3.state != "India").order_by(df3.sum_vaccinated.desc).limit(10)
    result4 = df4.to_pandas()
    # print(result4)
    showdata.draw_chart4(result4)

    #5.找出印度接种疫苗最少的10个州
    df5 = df3.select(df3.state,df3.sum_vaccinated).order_by(df3.sum_vaccinated.asc).limit(10)
    result5 = df5.to_pandas()
    # print(result5)
    showdata.draw_chart5(result5)

    #显示所有列
    pd.set_option('display.max_columns', 100)
    #6.统计截止到5.31各州的男女（变性人）接种总数
    df6 = input_table.filter(input_table.state != "India").group_by("state,updated_on").select(input_table.updated_on,input_table.state,input_table.male.sum,input_table.female.sum,input_table.transgender.sum,input_table.total_individuals_vaccinated.sum).where(input_table.updated_on == "2021-05-31")
    df6 = df6.drop_columns(df6.updated_on)
    df6 = df6.alias("state,sum_male,sum_female,sum_transgender,sum_total")
    result6 = df6.to_pandas()
    print(result6)
    showdata.draw_chart6(result6)

    #7.接种率
    #印度总人口1,383,500,000，截止到5.1日数据，来自维基百科
    df7 = input_table.filter(input_table.state == "India").filter(input_table.updated_on <= "2021-05-01").group_by(input_table.updated_on).select(input_table.updated_on,input_table.first_dose.sum).where(input_table.updated_on == "2021-05-01")
    df7 = df7.alias("state,sum")
    result7 = df7.to_pandas()
    print(result7)
    showdata.draw_chart7(result7)

    #8.印度各州每日的接种年龄分布（2021-03-16到2021-05-31）比例
    df8 = input_table.filter(input_table.updated_on >= "2021-03-16").group_by("state,updated_on").select(input_table.updated_on,input_table.state,input_table.first_age.sum,input_table.second_age.sum,input_table.third_age.sum,input_table.total_individuals_vaccinated.sum).where(input_table.updated_on == "2021-05-31")
    df8 = df8.alias("updated_on,state,sum1,sum2,sum3,sum")
    df8 = df8.add_columns((df8.sum1 / df8.sum).alias("rate_1"))
    df8 = df8.add_columns((df8.sum2 / df8.sum).alias("rate_2"))
    df8 = df8.add_columns((df8.sum3 / df8.sum).alias("rate_3"))
    result8 = df8.to_pandas()
    print(result8)
    showdata.draw_chart8(result8)
