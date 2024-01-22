# mekari_test

## Instructions on how to setup and run the program
Here, I'm using `Airflow` as a scheduler to execute Python code

Im installing using `astronomer` you can see the documentation in here: https://docs.astronomer.io/astro/cli/install-cli <br>
or you can follow the instructions below using homebrew: <br><br>
1. Install astronomer <br>
`brew install astro`<br><br>
2. Create new directory (in my case i create new project directory called **airflow_astro**) then run:<br>
`astro dev init`<br><br>
3. Run the astronomer and see the localhost for airflow run locally http://localhost:8080/<br>
`astro dev start`<br><br>
4. Copy all files in `dags` folder (https://github.com/jodiesamuel/mekari_test/tree/main/dags) to `dags` in local astronomer project<br><br>
5. Open the `Airflow` local and run the pipeline<br><br>
<img width="373" alt="image" src="https://github.com/jodiesamuel/mekari_test/assets/31727419/c88eeb70-ed17-4131-891d-01239fd4201a"><br><br>

To see the results, please check on PostgreSQL using GUI apps, for example, DBeaver. the credentials you can use:<br>
- Host: `localhost`<br>
- Database: `postgres`<br>
- Username: `postgres`<br>
- Password: `postgres`<br>
*Notes: the reason i'm using PostgreSQL you can be found in here:<br>
https://docs.google.com/document/d/1ptCbeERk-FSruDO5ul0JTYiuBkZCcf_4TcAzCU4N0cU/edit?usp=sharing<br>
Include with every question that I'm going to answer. <br><br>

If facing some errors with dependencies please stop the astronomer first using this line of code:<br>
`astro dev stop`<br><br>
Then copy file `dags/requirements.txt` into your dags folder and run the astronomer again <br>
`astro dev start`






