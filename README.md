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
<img width="373" alt="image" src="https://github.com/jodiesamuel/mekari_test/assets/31727419/c88eeb70-ed17-4131-891d-01239fd4201a">







