# Infranaut - Frontend Architecture

## 🏗️ **Frontend Architecture Overview**

The Infranaut frontend is built as a modern React/TypeScript application with a modular, component-based architecture designed for scalability, maintainability, and excellent user experience.

## 📋 **Phase Structure**

### **Phase 1: UI Design** 📋
**Status**: Planned
**Purpose**: Core UI components, design system, and layouts

### **Phase 2: API Integration** 📋
**Status**: Planned
**Purpose**: Backend API integration and state management

### **Phase 3: User Authentication** 📋
**Status**: Planned
**Purpose**: Authentication, authorization, and user management

### **Phase 4: Dashboard & Reporting** 📋
**Status**: Planned
**Purpose**: Advanced dashboards, charts, and reporting tools

## 🎨 **Phase 1: UI Design Architecture**

### **Component Structure**

```
src/
├── components/           # Reusable UI components
│   ├── common/          # Basic UI elements
│   │   ├── Button/
│   │   ├── Input/
│   │   ├── Modal/
│   │   └── Loading/
│   ├── layout/          # Layout components
│   │   ├── Header/
│   │   ├── Sidebar/
│   │   ├── Footer/
│   │   └── Navigation/
│   ├── forms/           # Form components
│   │   ├── FormField/
│   │   ├── FormSection/
│   │   └── Validation/
│   └── data/            # Data display components
│       ├── Table/
│       ├── Chart/
│       ├── Card/
│       └── Badge/
├── hooks/               # Custom React hooks
├── utils/               # Utility functions
├── styles/              # Global styles and themes
└── types/               # TypeScript type definitions
```

### **Design System**

#### **Color Palette**
```typescript
// Theme colors
const colors = {
  primary: {
    50: '#eff6ff',
    500: '#3b82f6',
    900: '#1e3a8a'
  },
  secondary: {
    50: '#f8fafc',
    500: '#64748b',
    900: '#0f172a'
  },
  success: {
    50: '#f0fdf4',
    500: '#22c55e',
    900: '#14532d'
  },
  warning: {
    50: '#fffbeb',
    500: '#f59e0b',
    900: '#78350f'
  },
  error: {
    50: '#fef2f2',
    500: '#ef4444',
    900: '#7f1d1d'
  }
};
```

#### **Typography**
```typescript
// Typography scale
const typography = {
  h1: {
    fontSize: '2.5rem',
    fontWeight: 700,
    lineHeight: 1.2
  },
  h2: {
    fontSize: '2rem',
    fontWeight: 600,
    lineHeight: 1.3
  },
  body: {
    fontSize: '1rem',
    fontWeight: 400,
    lineHeight: 1.5
  },
  caption: {
    fontSize: '0.875rem',
    fontWeight: 400,
    lineHeight: 1.4
  }
};
```

#### **Spacing System**
```typescript
// Spacing scale
const spacing = {
  xs: '0.25rem',   // 4px
  sm: '0.5rem',    // 8px
  md: '1rem',      // 16px
  lg: '1.5rem',    // 24px
  xl: '2rem',      // 32px
  xxl: '3rem'      // 48px
};
```

### **Component Examples**

#### **Button Component**
```typescript
// components/common/Button/Button.tsx
interface ButtonProps {
  variant: 'primary' | 'secondary' | 'outline' | 'ghost';
  size: 'sm' | 'md' | 'lg';
  disabled?: boolean;
  loading?: boolean;
  children: React.ReactNode;
  onClick?: () => void;
}

export const Button: React.FC<ButtonProps> = ({
  variant,
  size,
  disabled,
  loading,
  children,
  onClick
}) => {
  return (
    <button
      className={`btn btn-${variant} btn-${size}`}
      disabled={disabled || loading}
      onClick={onClick}
    >
      {loading && <Spinner size="sm" />}
      {children}
    </button>
  );
};
```

#### **Data Table Component**
```typescript
// components/data/Table/DataTable.tsx
interface DataTableProps<T> {
  data: T[];
  columns: Column<T>[];
  pagination?: PaginationConfig;
  sorting?: SortingConfig;
  onRowClick?: (row: T) => void;
}

export const DataTable = <T extends Record<string, any>>({
  data,
  columns,
  pagination,
  sorting,
  onRowClick
}: DataTableProps<T>) => {
  return (
    <div className="data-table">
      <table>
        <thead>
          <tr>
            {columns.map(column => (
              <th key={column.key}>{column.title}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, index) => (
            <tr key={index} onClick={() => onRowClick?.(row)}>
              {columns.map(column => (
                <td key={column.key}>{column.render(row)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
```

## 🔌 **Phase 2: API Integration Architecture**

### **API Service Layer**

#### **API Client**
```typescript
// services/api/client.ts
class ApiClient {
  private baseURL: string;
  private token: string | null;

  constructor(baseURL: string) {
    this.baseURL = baseURL;
    this.token = localStorage.getItem('auth_token');
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseURL}${endpoint}`;
    const headers = {
      'Content-Type': 'application/json',
      ...(this.token && { Authorization: `Bearer ${this.token}` }),
      ...options.headers
    };

    const response = await fetch(url, {
      ...options,
      headers
    });

    if (!response.ok) {
      throw new Error(`API Error: ${response.status}`);
    }

    return response.json();
  }

  async get<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint);
  }

  async post<T>(endpoint: string, data: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  async put<T>(endpoint: string, data: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: 'PUT',
      body: JSON.stringify(data)
    });
  }

  async delete<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, {
      method: 'DELETE'
    });
  }
}
```

#### **Service Modules**
```typescript
// services/api/dataCollection.ts
export class DataCollectionService {
  constructor(private client: ApiClient) {}

  async getDataSources(): Promise<DataSource[]> {
    return this.client.get<DataSource[]>('/api/v1/ingestion/sources');
  }

  async addDataSource(source: CreateDataSourceRequest): Promise<DataSource> {
    return this.client.post<DataSource>('/api/v1/ingestion/sources', source);
  }

  async getValidationResults(datasetId: string): Promise<ValidationResult[]> {
    return this.client.get<ValidationResult[]>(`/api/v1/validation/results/${datasetId}`);
  }

  async validateData(data: any[], schema: string): Promise<ValidationResponse> {
    return this.client.post<ValidationResponse>('/api/v1/validation/validate', {
      data,
      schema
    });
  }
}
```

### **State Management**

#### **Redux Store Structure**
```typescript
// store/index.ts
import { configureStore } from '@reduxjs/toolkit';
import dataCollectionReducer from './slices/dataCollectionSlice';
import validationReducer from './slices/validationSlice';
import storageReducer from './slices/storageSlice';
import metadataReducer from './slices/metadataSlice';

export const store = configureStore({
  reducer: {
    dataCollection: dataCollectionReducer,
    validation: validationReducer,
    storage: storageReducer,
    metadata: metadataReducer
  },
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: false
    })
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
```

#### **Redux Slice Example**
```typescript
// store/slices/dataCollectionSlice.ts
import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { DataCollectionService } from '../../services/api/dataCollection';

interface DataCollectionState {
  dataSources: DataSource[];
  loading: boolean;
  error: string | null;
}

const initialState: DataCollectionState = {
  dataSources: [],
  loading: false,
  error: null
};

export const fetchDataSources = createAsyncThunk(
  'dataCollection/fetchDataSources',
  async (service: DataCollectionService) => {
    return await service.getDataSources();
  }
);

const dataCollectionSlice = createSlice({
  name: 'dataCollection',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchDataSources.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(fetchDataSources.fulfilled, (state, action) => {
        state.loading = false;
        state.dataSources = action.payload;
      })
      .addCase(fetchDataSources.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || 'Failed to fetch data sources';
      });
  }
});

export default dataCollectionSlice.reducer;
```

### **Custom Hooks**
```typescript
// hooks/useApi.ts
import { useState, useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { RootState, AppDispatch } from '../store';

export const useApi = <T>(
  selector: (state: RootState) => T,
  action: () => any
) => {
  const dispatch = useDispatch<AppDispatch>();
  const data = useSelector(selector);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    setIsLoading(true);
    dispatch(action())
      .finally(() => setIsLoading(false));
  }, [dispatch, action]);

  return { data, isLoading };
};
```

## 🔐 **Phase 3: User Authentication Architecture**

### **Authentication Context**
```typescript
// contexts/AuthContext.tsx
interface AuthContextType {
  user: User | null;
  login: (credentials: LoginCredentials) => Promise<void>;
  logout: () => void;
  isAuthenticated: boolean;
}

export const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  const login = async (credentials: LoginCredentials) => {
    try {
      const response = await authService.login(credentials);
      setUser(response.user);
      localStorage.setItem('auth_token', response.token);
    } catch (error) {
      throw error;
    }
  };

  const logout = () => {
    setUser(null);
    localStorage.removeItem('auth_token');
  };

  useEffect(() => {
    // Check for existing token and validate
    const token = localStorage.getItem('auth_token');
    if (token) {
      authService.validateToken(token)
        .then(user => setUser(user))
        .catch(() => logout())
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, []);

  return (
    <AuthContext.Provider value={{
      user,
      login,
      logout,
      isAuthenticated: !!user
    }}>
      {children}
    </AuthContext.Provider>
  );
};
```

### **Protected Routes**
```typescript
// components/auth/ProtectedRoute.tsx
interface ProtectedRouteProps {
  children: React.ReactNode;
  requiredRoles?: string[];
}

export const ProtectedRoute: React.FC<ProtectedRouteProps> = ({
  children,
  requiredRoles = []
}) => {
  const { user, isAuthenticated } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (!isAuthenticated) {
      navigate('/login');
    } else if (requiredRoles.length > 0 && !requiredRoles.includes(user?.role)) {
      navigate('/unauthorized');
    }
  }, [isAuthenticated, user, requiredRoles, navigate]);

  if (!isAuthenticated) {
    return <LoadingSpinner />;
  }

  return <>{children}</>;
};
```

## 📊 **Phase 4: Dashboard & Reporting Architecture**

### **Dashboard Components**

#### **Dashboard Layout**
```typescript
// components/dashboard/DashboardLayout.tsx
interface DashboardLayoutProps {
  children: React.ReactNode;
  sidebarItems: SidebarItem[];
}

export const DashboardLayout: React.FC<DashboardLayoutProps> = ({
  children,
  sidebarItems
}) => {
  return (
    <div className="dashboard-layout">
      <Header />
      <div className="dashboard-content">
        <Sidebar items={sidebarItems} />
        <main className="dashboard-main">
          {children}
        </main>
      </div>
    </div>
  );
};
```

#### **Chart Components**
```typescript
// components/charts/LineChart.tsx
interface LineChartProps {
  data: ChartDataPoint[];
  xAxis: string;
  yAxis: string;
  title: string;
  height?: number;
}

export const LineChart: React.FC<LineChartProps> = ({
  data,
  xAxis,
  yAxis,
  title,
  height = 400
}) => {
  const chartRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (chartRef.current) {
      const chart = echarts.init(chartRef.current);
      
      const option = {
        title: { text: title },
        xAxis: { type: 'category', data: data.map(d => d[xAxis]) },
        yAxis: { type: 'value' },
        series: [{
          data: data.map(d => d[yAxis]),
          type: 'line'
        }]
      };

      chart.setOption(option);
    }
  }, [data, xAxis, yAxis, title]);

  return <div ref={chartRef} style={{ height }} />;
};
```

### **Real-time Updates**
```typescript
// hooks/useWebSocket.ts
export const useWebSocket = <T>(url: string) => {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('WebSocket connected');
    };

    ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      setData(message);
    };

    ws.onerror = (error) => {
      setError('WebSocket error');
    };

    return () => {
      ws.close();
    };
  }, [url]);

  const sendMessage = (message: any) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(message));
    }
  };

  return { data, error, sendMessage };
};
```

## 🎨 **Styling Architecture**

### **CSS-in-JS with Styled Components**
```typescript
// styles/components/Button.styles.ts
import styled from 'styled-components';

export const StyledButton = styled.button<ButtonProps>`
  padding: ${props => props.size === 'sm' ? '8px 16px' : '12px 24px'};
  border-radius: 6px;
  border: none;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;

  background-color: ${props => {
    switch (props.variant) {
      case 'primary': return props.theme.colors.primary[500];
      case 'secondary': return props.theme.colors.secondary[500];
      default: return 'transparent';
    }
  }};

  color: ${props => props.variant === 'outline' ? props.theme.colors.primary[500] : 'white'};

  &:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  }

  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
    transform: none;
  }
`;
```

### **Theme Provider**
```typescript
// styles/theme.ts
export const theme = {
  colors: {
    primary: { /* color palette */ },
    secondary: { /* color palette */ },
    // ... other colors
  },
  typography: {
    // typography scale
  },
  spacing: {
    // spacing scale
  },
  breakpoints: {
    mobile: '320px',
    tablet: '768px',
    desktop: '1024px',
    wide: '1440px'
  }
};

// App.tsx
export const App: React.FC = () => {
  return (
    <ThemeProvider theme={theme}>
      <GlobalStyles />
      <Router>
        <Routes />
      </Router>
    </ThemeProvider>
  );
};
```

## 🚀 **Performance Optimization**

### **Code Splitting**
```typescript
// Lazy loading components
const Dashboard = lazy(() => import('./pages/Dashboard'));
const Reports = lazy(() => import('./pages/Reports'));
const Settings = lazy(() => import('./pages/Settings'));

// Route configuration
const routes = [
  {
    path: '/dashboard',
    element: (
      <Suspense fallback={<LoadingSpinner />}>
        <Dashboard />
      </Suspense>
    )
  }
];
```

### **Memoization**
```typescript
// Memoized components
export const ExpensiveChart = memo<ChartProps>(({ data, config }) => {
  const processedData = useMemo(() => {
    return processChartData(data, config);
  }, [data, config]);

  return <Chart data={processedData} />;
});
```

### **Virtual Scrolling**
```typescript
// Virtual scrolling for large datasets
export const VirtualTable: React.FC<VirtualTableProps> = ({ data, height }) => {
  const [scrollTop, setScrollTop] = useState(0);
  const itemHeight = 50;
  const visibleItems = Math.ceil(height / itemHeight);
  const startIndex = Math.floor(scrollTop / itemHeight);
  const endIndex = startIndex + visibleItems;

  const visibleData = data.slice(startIndex, endIndex);

  return (
    <div style={{ height, overflow: 'auto' }} onScroll={handleScroll}>
      <div style={{ height: data.length * itemHeight }}>
        <div style={{ transform: `translateY(${startIndex * itemHeight}px)` }}>
          {visibleData.map(item => (
            <TableRow key={item.id} data={item} />
          ))}
        </div>
      </div>
    </div>
  );
};
```

## 🧪 **Testing Strategy**

### **Unit Testing**
```typescript
// __tests__/components/Button.test.tsx
import { render, screen, fireEvent } from '@testing-library/react';
import { Button } from '../Button';

describe('Button Component', () => {
  it('renders with correct text', () => {
    render(<Button variant="primary">Click me</Button>);
    expect(screen.getByText('Click me')).toBeInTheDocument();
  });

  it('calls onClick when clicked', () => {
    const handleClick = jest.fn();
    render(<Button variant="primary" onClick={handleClick}>Click me</Button>);
    
    fireEvent.click(screen.getByText('Click me'));
    expect(handleClick).toHaveBeenCalledTimes(1);
  });
});
```

### **Integration Testing**
```typescript
// __tests__/integration/Dashboard.test.tsx
import { render, screen, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { store } from '../../store';
import { Dashboard } from '../../pages/Dashboard';

describe('Dashboard Integration', () => {
  it('loads and displays dashboard data', async () => {
    render(
      <Provider store={store}>
        <Dashboard />
      </Provider>
    );

    await waitFor(() => {
      expect(screen.getByText('Model Performance')).toBeInTheDocument();
    });
  });
});
```

---

**This frontend architecture provides a modern, scalable, and maintainable foundation for the Infranaut AI Model Monitoring & Observability Platform user interface.**
