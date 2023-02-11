高效的测试数据生成工具
## 优点
- 利用golang的高并发模型使用更少的内存并且保证数据生成效率,单机可达30w eps
- 支持多系统 window,linux,mac
- 可控制生成速度比如可以指定1000/s
- 配置文件可重复使用,通过上一份配置文件会产生一份新的配置文件,如第一份配置文件只指定生成20个用户,如果此时停掉工具再次使用该配置就会又重新生成20个用户数据里就有40个用户
  如果还想要保持之前的20个用户就可以使用程序新生成的配置文件
- 支持数据类型丰富,还可以使用struct类型自己指定枚举值

## 可以使用字段类型

| 字段类型      |                      |
| ------------- | -------------------- |
| struct        | 可指定任意值字段类型 |
| username      | 用户名               |
| email         | 邮箱                 |
| ip4           | ip4地址              |
| ip6           | ip6地址              |
| url           | 网址                 |
| phone         | 电话                 |
| uuid          | 随机id               |
| domain        | 域名                 |
| fruit         | 水果                 |
| macaddress    | mac地址              |
| country       | 国家                 |
| city          | 城市                 |
| street        | 街道                 |
| httpcode      | 网站返回状态码       |
| httpmethod    | 请求方法 get post    |
| useragent     | 浏览器头             |
| company       | 公司                 |
| joblevel      | 工作级别             |
| jobtitle      | 工作title            |
| jobdescriptor | 工作                 |
| appname       | 应用                 |
| appversion    | 应用版本             |
| datetime      | 时间字段             |

## 配置样例
参考config.ini和fields.ini


