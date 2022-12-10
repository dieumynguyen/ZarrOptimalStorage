import numpy as np
import seaborn as sns
from scipy.stats import linregress
sns.set(style='ticks', palette='Set2')

def plot_pairwise_metrics(ax, metrics_df, x_var, y_var, 
                        color_palette, titles,
                        log=True, legend=False, fontsize=13, 
                        plot_lines=False, ylim=10):
    line_colors = ['b', 'r']
    g = sns.scatterplot(data=metrics_df, x=x_var, y=y_var, 
                    hue='lonlat_product', palette=color_palette,
                    s=35, alpha=0.5 if plot_lines else 0.9, 
                    ax=ax, legend=legend, style='dataset',
                    markers=['o', '^'])

    if plot_lines:
        # Plot best fit lines for each group of lonlat_product
        groups = metrics_df['dataset'].unique()
        for group_i, group in enumerate(groups):
            x = metrics_df[x_var][metrics_df['dataset']==group].values #.reshape(-1, 1)
            y = metrics_df[y_var][metrics_df['dataset']==group].values #.reshape(-1, 1)
            logx = np.log(x)
            logy = np.log(y)
            m, c, r, p, se = linregress(logx, logy)
            print(f'Group: {group} - Slope: {m} - p-value: {p}')
            y_fit = np.exp(m*logx + c) # calculate the fitted values of y 
            x_min_idx = np.argmin(x)
            x_max_idx = np.argmax(x)
            ax.plot([x[x_min_idx], x[x_max_idx]], [y_fit[x_min_idx], y_fit[x_max_idx]], 
                    linewidth=2, alpha=0.55, color=line_colors[group_i], 
                   linestyle='-' if p<0.05 else '--')
            
        # Plot overall line
        x = metrics_df[x_var].values #.reshape(-1, 1)
        y = metrics_df[y_var].values #.reshape(-1, 1)
        logx = np.log(x)
        logy = np.log(y)
        m, c, r, p, se = linregress(logx, logy)
        print(f'Group: Both - Slope: {m} - p-value: {p} \n')
        y_fit = np.exp(m*logx + c) # Calculate the fitted values of y 
        x_min_idx = np.argmin(x)
        x_max_idx = np.argmax(x)
        ax.plot([x[x_min_idx], x[x_max_idx]], [y_fit[x_min_idx], y_fit[x_max_idx]], 
                linewidth=3, alpha=0.8, color='purple', 
                linestyle='-' if p<0.05 else '--')
        
    if log:
        ax.set_xscale('log')
        ax.set_yscale('log')
        ax.set_ylim((ylim[0], ylim[1]))
        ax.set_xlabel(f'log10({titles[x_var]})', fontsize=fontsize, labelpad=0.25)
        ax.set_ylabel(f'log10({titles[y_var]})', fontsize=fontsize, labelpad=0.25)
    else:
        ax.set_xlabel(titles[x_var], fontsize=fontsize)
        ax.set_ylabel(titles[y_var], fontsize=fontsize)
        
    if legend:
        g.legend(loc='lower center', bbox_to_anchor=(1.25, 0.5), 
                 title='Longitude chunk size x Lattitude chunk size', ncol=7)
