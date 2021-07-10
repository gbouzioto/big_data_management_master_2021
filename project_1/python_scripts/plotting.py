import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sns.set()


def plot_results(question=None, file='part-r-00000'):
    '''
    Plots a summary of the results for each question.
    '''

    if question == 1:
        # Read results
        q1 = pd.read_csv(f'question{question}/{file}', sep='\t', header=None,
                         names=['ID', 'Name', 'Sex', 'Gold Medals'])

        # Get the top athletes
        q1_top = q1.sort_values('Gold Medals', ascending=False).iloc[:6, :]

        # Groupby Sex
        q1_sex = q1.groupby('Sex')['Gold Medals'].sum().to_frame()
        q1_sex.reset_index(inplace=True)

        # Plot results
        fig, ax = plt.subplots(2, 1, figsize=(13, 12))

        sns.barplot(data=q1_top, y='Name', x='Gold Medals', ax=ax[0])
        ax[0].set_title('Athletes with the most gold medals in total', fontsize=13)

        sns.barplot(data=q1_sex, x='Sex', y='Gold Medals', ax=ax[1])
        ax[1].set_title('Total Gold Medals per sex', fontsize=13)

        return plt.show()

    elif question == 2:
        # Read results
        q2 = pd.read_csv(f'question{question}/{file}', sep='\t', header=None,
                         names=['Rank', 'Name', 'Sex', 'Age', 'Team','Sport', 'Games',
                                'Gold Medals', 'Silver Medals', 'Bronze Medals', 'Total Medals'])

        # Get Athlete, Games column
        q2['Athlete, Games'] = q2['Name'] + ', ' + q2['Games'].str[:4]

        # Select relevant columns
        q2_sel = q2[['Athlete, Games', 'Gold Medals', 'Total Medals']]
        q2_sel = q2_sel.iloc[::-1]

        # Plot results
        fig = q2_sel.set_index('Athlete, Games').plot(kind='barh', stacked=True, figsize=(13, 11), title='Top athletes of all time')
        fig.title.set_size(13)

        return plt.show()

    elif question == 3:
        # Read results
        q3 = pd.read_csv(f'question{question}/{file}', sep='\t', header=None,
                         names=['Games', 'Team', 'NOC', 'Female Athletes', 'Sport_1', 'Sport_2'])

        # Get year column
        q3['Year'] = q3['Games'].str.split().str[0]

        # Groupby by Year
        year_female = q3.groupby('Year')['Female Athletes'].mean().to_frame()
        year_female.reset_index(inplace=True)

        # Select every two olympic games
        N = 2
        year_female_grouped = year_female.groupby(year_female.index // N).sum(as_index=False)
        year_female_grouped['Year'] = year_female_grouped['Year'].str[:4] + '-' + year_female_grouped['Year'].str[4:]

        # Plot results
        plt.figure(figsize=(13, 8))

        plt.title('Participation of female athletes over the years', fontsize=13)

        sns.lineplot(data=year_female_grouped, x='Year', y='Female Athletes')

        plt.xticks(rotation=30)

        return plt.show()

    else:
        raise ValueError('Select a valid question for plotting.')