# Ejecución de comandos SQL asincrónicos en serie y en paralelo

Por: Mauricio Cotes

Junio 16 de 2019

## Resumen
Este texto intenta explicar la implementación de una forma simple de ejecutar comandos SQL sin tener que esperar a que estos terminen. Es especialmente útil para comandos de larga duración en su ejecución, donde no es conveniente que el usuario se quede esperando a su finalización, así, el comando es enviado y el control se devuelve al programa que lo invoca de modo que este pueda proseguir con su ejecución. No obstante, en el modelo que aquí se presenta, el programa que los invoca debe eventualmente, preguntar por el estado de las ejecuciones de los comandos para verificar si han concluido. La invocación del comando representa el envío de un mensaje al sistema de ejecución asincrónica, el cual debe ser procesado en algún momento posteriormente. La invocación constituye la primera parte del procesamiento. La segunda parte, consiste en el procesamiento en sí de los comandos, el cual se lleva a cabo en el mismo orden en que fueron invocados, es decir a la manera de una cola. 

Asumiendo la metáfora de una cola, si solo hay un punto de atención, los comandos se procesan de forma serial y un segundo comando debe esperar a que finalice la atención del primero. Si hay varios puntos de atención para una cola, varios comandos pueden ser atendidos simultáneamente en paralelo, pero con la condición que estos no pueden ser dependientes entre sí, o sea que el segundo comando no debe necesitar que se ejecute antes el primero. Este modo de atención en paralelo disminuye dramáticamente el tiempo de espera promedio de los elementos en la cola.

La implementación que se describe fue realizada sobre SQL Server 2017, aunque la idea general podría ser implementada en cualquier entorno de programación. SQL Server no incluye esta funcionalidad asincrónica out-of-the-box, aunque proporciona varias posibilidades de infraestructura para ser implementada. Algunas de ellas incluyen: SQL Server Service Broker, SQL Server Agent y quizás, con un poco más de esfuerzo, SQL Server Integration Services. La implementación presentada en el texto actual se basa en SQL Server Agent.

## Introducción e instalación
La ejecución de comandos SQL asincrónicos puede ser de gran utilidad en ambientes empresariales que involucren bases de datos. El modelo que se presenta es simple, en tanto que puede ser utilizado sin mayores requerimientos. Para su uso, basta con ejecutar el script que se encuentra en el apéndice sobre una base de datos SQL Server que se designe para este propósito. En el numeral de **Arquitectura del sistema de ejecución asincrónica**, se revisa con más detalle la forma en que se ha construido el sistema que se presenta, pero se ha preferido explicar antes como puede usarse el sistema. 

Antes de empezar es necesario hacer algunas consideraciones en cuanto a los requerimientos del sistema:
* Versiones soportadas: SQL Server 2016 o posterior. Para versiones anteriores es posible usarlos con muy pocos ajustes.
* Se requiere que SQL Server Agent esté activo para la instancia de SQL Server donde se instale el sistema.
* La propiedad _Contaiment type_ de la base de datos donde se instale debe ser ‘NONE’. De aquí se deduce que el sistema de ejecución asincrónica tiene dependencias externas y, en efecto, se basa en el subsistema del SQL Server Agent y en la correspondiente base de datos msdb.

El **procedimiento de instalación**, como ya se mencionó, consiste en ejecutar el Script ("Ejecución de comandos asincrónicos en T-SQL.sql") sobre la base de datos SQL Server que se quiera habilitar para este efecto. Esta acción instalará los siguientes elementos sobre la base de datos:

* **Esquema**: async (todos los objetos del sistema quedan bajo este esquema).
* **Tablas**: async.SpoolConfiguration y async.CommandsHistory.
* **Procedimientos almacenados**:
  * DropSpool
  * ExecuteCmd
  * PollPendingCommands
  * ProcessSpooledCmd
  * SetSpoolConfiguration
  * StartAgents
  * StopAgents
* **Funciones Table-valued**:
  * GetExecutedCommands
  * GetCmdExecStatus
  * GetPendingCommands
* **Funciones escalares**:
  * NumOfAgentsRunning
  * NumOfPendingCommands

Se ha intentado hacer la explicación comenzando desde lo simple y poco a poco ir incorporando nuevos elementos, como se verá en los numerales de la documentación. 
