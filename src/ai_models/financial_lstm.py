import torch
import torch.nn as nn


class MultiHeadRegressionLSTM(nn.Module):
    def __init__(self, input_dim, num_stocks, target_cols, hidden_dim, num_layers, dropout, model_name):
        super(MultiHeadRegressionLSTM, self).__init__()
        self.model_name = model_name
        self.target_cols = target_cols  # Ora passiamo la lista dei nomi (es. ['rev_1d', 'rev_5d'])

        self.stock_embedding = nn.Embedding(num_stocks, 24)

        self.lstm = nn.LSTM(
            input_size=input_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0
        )

        # Creiamo una "testa" separata per ogni target
        self.heads = nn.ModuleDict({
            target: nn.Sequential(
                nn.Linear(hidden_dim + 24, hidden_dim // 4),
                nn.LeakyReLU(0.1),
                nn.Linear(hidden_dim // 4, 1)  # Ogni testa sputa 1 solo valore
            ) for target in target_cols
        })

    def forward(self, x_tech, x_stock_id):
        lstm_out, _ = self.lstm(x_tech)
        last_step = lstm_out[:, -1, :]

        stock_emb = self.stock_embedding(x_stock_id)
        combined = torch.cat((last_step, stock_emb), dim=1)

        # Iteriamo su ogni testa e concateniamo i risultati nell'ordine corretto
        outputs = [self.heads[target](combined) for target in self.target_cols]
        return torch.cat(outputs, dim=1)  # Ritorna un tensore [Batch, len(target_cols)]


# Factory aggiornate
def get_regression_medium(input_dim, num_stocks, target_cols):
    return MultiHeadRegressionLSTM(input_dim, num_stocks, target_cols, 128, 2, 0.2, "RegMedium_MH")


def get_regression_deep(input_dim, num_stocks, target_cols):
    return MultiHeadRegressionLSTM(input_dim, num_stocks, target_cols, 128, 4, 0.3, "RegDeep_MH")


def get_regression_wide(input_dim, num_stocks, target_cols):
    return MultiHeadRegressionLSTM(input_dim, num_stocks, target_cols, 256, 2, 0.3, "RegWide_MH")