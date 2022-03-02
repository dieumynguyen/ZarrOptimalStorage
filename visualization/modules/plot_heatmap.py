import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as colors

def truncate_colormap(cmap, minval=0.0, maxval=1.0, n=100):
    new_cmap = colors.LinearSegmentedColormap.from_list(
        'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
        cmap(np.linspace(minval, maxval, n)))
    return new_cmap

def sort_pivot_table(df, results):
    # Sort pivot table columns by product (of lon x lat)
    # column_order = df["lon x lat"].loc[df.sort_values("lonlat_product").index].unique()
    column_order = ['10x10', '10x50', '50x10',
                    '10x100', '100x10', '50x50',
                    '50x100', '100x50', '100x100', 
                    '1152x721']
    results = results.reindex(column_order, axis=1)
    return results

def make_pivot_tables(metrics_df_1, metrics_df_2, 
                      column_name, convert_mb_to_gb, product):
    # Pivot table 1
    results_1 = metrics_df_1.pivot(index='time_chunks', columns='lon x lat', values=column_name)
    results_1 = sort_pivot_table(metrics_df_1, results_1)
    if column_name == 'peak_memories' and convert_mb_to_gb:
        results_1 = results_1 / 953.6743164062

    if metrics_df_2 is None:
        results = results_1
    else:
        # Pivot table 2
        results_2 = metrics_df_2.pivot(index='time_chunks', columns='lon x lat', values=column_name)
        results_2 = sort_pivot_table(metrics_df_2, results_2)
        if column_name == 'peak_memories' and convert_mb_to_gb:
            results_2 = results_2 / 953.6743164062

        if product:
            # Take product
            results = results_1 * results_2
        else:
             # Take avg
            results = (results_1 + results_2) / 2

    # Sort pivot table
    results = sort_pivot_table(metrics_df_2, results)
    
    return results

def make_heatmap(
    dataset,
    titles_dict,
    results,
    column_name,
    task_name,
    transpose=False, 
    vmin=None, 
    vmax=None, 
    quantile_q=None,
    fontsize=14,
    num_decimals=4,
    convert_mb_to_gb=False,
    show_title=False
):
    xlabel = "Longitude chunk size x Latitude chunk size"
    ylabel = "Time chunk size"
    
    if transpose:
        results = results.T
        xlabel, ylabel = ylabel, xlabel
    
    # Mask
    mask = np.zeros_like(results)
    mask[np.where(results==0)] = True
    mask[:] = True
    
    # Compute quantile
    if quantile_q is not None:
        result_vals = results.values
        result_vals = (result_vals[~np.isnan(result_vals)]).flatten()
        vmax = np.quantile(result_vals, q=quantile_q)
        
    # Get colormap
    cmap = plt.get_cmap('RdPu' if 'norm' in column_name else 'Reds')
    new_cmap = truncate_colormap(cmap, 0.0, 0.95, 100)
    
    # Plot
    plt.figure(figsize=(10,10), dpi=100)
    g = sns.heatmap(
        results, 
#         mask=mask, 
        annot=True, 
        annot_kws={"fontsize": fontsize},
        fmt=".2g" if 'norm' in column_name else f"0.{num_decimals}f", 
#         fmt=f"0.{num_decimals}f", 
        cmap=new_cmap, 
        square=True,
        linewidths=5, linecolor='white',
        cbar_kws={"label": "", "shrink": 0.85, "pad": 0.01},
        vmin=vmin,
        vmax=vmax,
        alpha=1 if 'norm' in column_name else 0.9,
    )
    g.invert_yaxis()
    g.set_xlabel(xlabel)
    g.set_ylabel(ylabel)
    
    plt.tick_params(
        axis='both',      # Changes apply to the x-axis
        which='both',     # Both major and minor ticks are affected
        bottom=True,      # Ticks along the bottom edge are off
        left=True
    ) 
    
    if show_title:
        plt.title(titles_dict[column_name])

    # Save heatmap
    if 'norm' in column_name:
        plt.savefig(f'../data/{dataset}/normalized_heatmaps/{task_name}_{column_name}.png', bbox_inches='tight')
    else:
        plt.savefig(f'../data/{dataset}/heatmaps/{task_name}_{column_name}.png', bbox_inches='tight')